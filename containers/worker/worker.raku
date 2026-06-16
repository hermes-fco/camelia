#!/usr/bin/env raku
# 🌺 Camélia PoC #7 — Worker Agent (JetStream pull consumer)
#
# Pulls tasks from JetStream WORKER_TASKS stream.
# Self-terminates after 15 min idle.
# Publishes worker.status.> events for spawner lifecycle tracking.

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url   = %*ENV<NATS_URL>   // 'nats://127.0.0.1:4222';
my $worker-id  = ('a'..'z').pick(6).join;
my $worker-type = %*ENV<SERVICE_NAME> // 'worker-generic';
$worker-type = $worker-type.subst(/^ 'worker-' /, '');

my $idle-timeout = 900;  # 15 minutes
my $last-activity = now.Int;

note "🟡 Worker {$worker-type}:{$worker-id} connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 Worker {$worker-type}:{$worker-id} connected.";

# ── JetStream pull consumer ──
my $stream = Nats::Stream.new:
    :$nats,
    :name<WORKER_TASKS>,
    ;

my $consumer = Nats::Consumer.new:
    :$nats,
    :name("worker-{$worker-id}"),
    :stream<WORKER_TASKS>,
    :ack-policy<explicit>,
    :deliver-policy<all>,
    :filter-subject('worker.task.>'),
    :max-ack-pending(1),
    :ack-wait(30),
    :replay-policy<instant>,
    :inactive-threshold($idle-timeout),
    ;
note "📥 Creating durable consumer worker-{$worker-id}...";
my $create-supply = $consumer.create-named;
my $create-msg = await $create-supply.Promise;
if $create-msg && $create-msg.payload && !$create-msg.payload.starts-with('-ERR') {
    note "✅ Consumer created, pulling tasks (idle timeout={$idle-timeout}s)...";
} else {
    note "⚠️ Consumer create: {$create-msg.?payload // 'no response'}";
}

# ── Publish startup status ──
$nats.publish: "worker.status.{$worker-type}.{$worker-id}.started", to-json({
    :worker_id($worker-id), :type($worker-type), :status<started>,
});

# ── Tool execution helper ──
sub exec-tool(Str $name, $tc-id, %args --> Hash) {
    my $sub   = $nats.subscribe: my $inbox = "_INBOX.tl." ~ (^1_000_000).pick, :1max-messages;
    my $p     = $sub.supply.head.Promise;
    $nats.publish: "tools.exec.{$name}", to-json({ :$name, :tool_call_id($tc-id), :arguments(%args) }), :reply-to($inbox);
    await Promise.anyof: $p, Promise.in(30);
    $nats.unsubscribe: $sub;
    return { :error("Tool timeout") } unless $p.so;
    my $msg = $p.result;
    return { :error("Empty tool response") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("Bad tool JSON") };
}

# ── Model call helper ──
my @tools = (
    { type => "function", function => {
        name => "run_shell", description => "Execute a shell command, return \{stdout, stderr, exit_code\}",
        parameters => { type => "object", properties => {
            command => { type => "string", description => "Shell command" } }, required => ["command"] } } },
);

sub call-model(@messages --> Hash) {
    my $sub = $nats.subscribe: my $inbox = "_INBOX.wkr." ~ (^1_000_000).pick, :1max-messages;
    my $p   = $sub.supply.head.Promise;
    $nats.publish: 'model.deepseek.completion', to-json({
        :model('deepseek-v4-pro'), :@messages, :@tools, :tool_choice<auto>,
    }), :reply-to($inbox);
    await Promise.anyof: $p, Promise.in(120);
    $nats.unsubscribe: $sub;
    return { :error("Model timeout") } unless $p.so;
    my $msg = $p.result;
    return { :error("Empty model response") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("JSON parse fail") };
}

my $system = q:to/END/;
You are a specialized worker agent. Complete the assigned task using available tools.
Be thorough — don't leave work half-done. Return a complete result.
END

react {
    # ── Pull messages from JetStream ──
    whenever $consumer.msgs(:expires($idle-timeout), :no-wait) -> $msg {
        next unless $msg.payload;

        # Check for JetStream errors (408 timeout, etc.)
        if $msg.payload.starts-with('-ERR') {
            note "⚠️ JetStream: {$msg.payload}";
            last;
        }

        my %task;
        try { %task = from-json($msg.payload) };
        if $! {
            note "⚠️ Invalid JSON payload: {$!.message.substr(0, 80)}";
            $consumer.nak($msg);
            next;
        }

        my $task-text = %task<task> // '';
        my $role      = %task<role> // 'worker';
        my $task-id   = %task<id>   // 'unknown';
        my $reply-to  = %task<reply-to>;

        unless $reply-to {
            note "⚠️ Task {$task-id} without reply-to, skipping";
            $consumer.ack($msg);
            next;
        }

        note "📨 {$worker-type}:{$worker-id}: task {$task-id} — {$task-text.substr(0, 80)}...";

        # ── Publish busy status ──
        $last-activity = now.Int;
        $nats.publish: "worker.status.{$worker-type}.{$worker-id}.busy", to-json({
            :worker_id($worker-id), :type($worker-type), :status<busy>, :$task-id,
        });

        # ⚠️ MUST use start {} — await inside react whenever blocks the event loop,
        # preventing the model response subscription from receiving messages.
        start {
            process-task($task-id, $role, $task-text, $reply-to, $msg);
        }
    }

    # ── Idle timer: self-terminate if inactive for too long ──
    whenever Supply.interval(30) {
        my $idle = now.Int - $last-activity;
        if $idle > $idle-timeout {
            note "⏰ {$worker-type}:{$worker-id} idle for {$idle}s > {$idle-timeout}s — self-terminating...";
            $nats.publish: "worker.status.{$worker-type}.{$worker-id}.idle", to-json({
                :worker_id($worker-id), :type($worker-type), :status<idle>, :reason<timeout>,
                :idle_seconds($idle),
            });
            # Give NATS a moment to flush the idle message
            sleep 0.5;
            note "👋 {$worker-type}:{$worker-id} goodbye.";
            exit(0);
        }
    }
}

sub process-task($task-id, $role, $task-text, $reply-to, $msg) {
    my @messages = (
        { :role<system>, :content($system ~ "\nYour role: {$role}") },
        { :role<user>,   :content($task-text) },
    );

    my $final = '';
    my $turns = 5;
    loop {
        last if $turns-- <= 0;
        my %resp = call-model(@messages);
        if %resp<error> { $final = "ERROR: {%resp<error>}"; last; }

        my $choice = %resp<choices>[0];
        last unless $choice;
        my $message = $choice<message> // {};
        my $finish  = $choice<finish_reason> // '';

        if $message<content> { $final = $message<content> }

        if $finish eq 'tool_calls' || $message<tool_calls> {
            @messages.push: $message;
            for $message<tool_calls>.List -> $tc {
                my $fn   = $tc<function>;
                my $name = $fn<name>;
                my %args = try from-json($fn<arguments>) // {};
                note "  🔧 {$name}";
                my %result = exec-tool($name, $tc<id>, %args);
                @messages.push: { :role<tool>, :tool_call_id($tc<id>), :content(to-json(%result)) };
            }
            next;
        }

        if $finish eq 'stop' { @messages.push: $message; last }
        last;  # unknown finish reason
    }

    $nats.publish: $reply-to, to-json({ :$worker-id, :$role, :$task-id, :result($final) });
    $consumer.ack($msg);
    note "✅ {$worker-type}:{$worker-id}: task {$task-id} done";

    # ── Publish idle status after task completion ──
    $last-activity = now.Int;
    $nats.publish: "worker.status.{$worker-type}.{$worker-id}.idle", to-json({
        :worker_id($worker-id), :type($worker-type), :status<idle>,
    });
}
