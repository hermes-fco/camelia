#!/usr/bin/env raku
# 🌺 Camélia PoC #4 — Worker Agent (JetStream pull consumer)
#
# Pulls tasks from JetStream consumer, processes via model+tool loop,
# idles for 5 minutes then self-terminates.
# Queue group ensures one task per worker.

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url  = %*ENV<NATS_URL>  // 'nats://127.0.0.1:4222';
my $worker-id = ('a'..'z').pick(6).join;  # random short ID

# ── Tools schema ──
my @tools = (
    {
        type     => "function",
        function => {
            name        => "run_shell",
            description => "Execute a shell command in the Linux sandbox and return stdout, stderr and exit code.",
            parameters  => {
                type       => "object",
                properties => {
                    command => { type => "string", description => "Shell command to execute" },
                },
                required => ["command"],
            },
        },
    },
    {
        type     => "function",
        function => {
            name        => "read_file",
            description => "Read a file from the sandbox and return its content with numbered lines.",
            parameters  => {
                type       => "object",
                properties => {
                    path   => { type => "string",  description => "File path (relative to sandbox)" },
                    offset => { type => "integer", description => "Starting line (0-indexed, default 0)" },
                    limit  => { type => "integer", description => "Max lines (default 500)" },
                },
                required => ["path"],
            },
        },
    },
    {
        type     => "function",
        function => {
            name        => "write_file",
            description => "Write content to a file in the sandbox.",
            parameters  => {
                type       => "object",
                properties => {
                    path    => { type => "string", description => "File path (relative to sandbox)" },
                    content => { type => "string", description => "Content to write" },
                },
                required => ["path", "content"],
            },
        },
    },
);

# ── System prompt ──
my $system = q:to/END/;
You are a specialized worker agent. Complete the task using the available tools.
Be thorough and precise. Deliver a complete result — don't leave work half-done.
END

note "🟡 Worker {$worker-id} connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 Worker {$worker-id} connected.";

# ── JetStream consumer (stream created by orchestrator) ──
my $stream = Nats::Stream.new:
    :$nats,
    :name<WORKER_TASKS>,
    :subjects(['worker.task.>']),
    ;

my $consumer = $stream.consumer:
    'worker-pool',
    :durable-name<worker-pool>,
    :filter-subject('worker.task.>'),
    :ack-policy<explicit>,
    :max-ack-pending(1),
    :ack-wait(30),
    ;

note "📥 Worker {$worker-id} ready to pull from JetStream.";

# ═════════════════════════════════════════════
# MAIN WORKER LOOP: pull → process → repeat
# ═════════════════════════════════════════════

my $idle-timeout = 300;  # 5 minutes
my $tasks-done = 0;

loop {
    note "⏳ Worker {$worker-id}: waiting for task (timeout={$idle-timeout}s)...";

    my $msg-supply = $consumer.next;
    my $msg = await $msg-supply.Promise;

    # next() returns a message or times out
    unless $msg && $msg.payload {
        note "⏰ Worker {$worker-id}: idle timeout, self-terminating after {$tasks-done} tasks.";
        last;
    }

    # Parse the task
    my %task = try from-json($msg.payload);
    if $! {
        note "⚠️ Invalid JSON, acking and skipping";
        ack($nats, $msg, $stream, $consumer);
        next;
    }

    my $task-text = %task<task> // 'Execute a task';
    my $role      = %task<role> // 'worker';
    my $task-id   = %task<id>   // 'unknown';
    note "📨 Worker {$worker-id}: task {$task-id} — {$task-text.substr(0, 100)}...";

    # Process the task
    my $result = process-one-task($nats, $task-text, $role, $worker-id);

    # Send result back if reply-to was provided
    if %task<reply-to> {
        $nats.publish: %task<reply-to>, to-json({
            :$worker-id,
            :$role,
            :$task-id,
            :$result,
        });
    }

    # Ack the JetStream message
    ack($nats, $msg, $stream, $consumer);
    $tasks-done++;

    note "✅ Worker {$worker-id}: task {$task-id} complete ({$tasks-done} total)";
}

note "💀 Worker {$worker-id} exiting.";
exit 0;

# ═════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════

sub ack($nats, $msg, $stream, $consumer) {
    $consumer.ack($msg) if $msg;
}

sub process-one-task($nats, Str $task, Str $role, Str $worker-id --> Str) {
    my @messages = (
        { :role<system>, :content($system ~ "\n\nYour role in this task: {$role}") },
        { :role<user>,   :content($task) },
    );

    my $final-content = '';
    my $max-turns = 8;

    loop {
        last if $max-turns-- <= 0;

        my %resp = call-model($nats, @messages);
        if %resp<error> {
            note "  ❌ Model error: {%resp<error>}";
            $final-content = "ERROR: {%resp<error>}";
            last;
        }

        my $choice = %resp<choices>[0];
        unless $choice {
            note "  ❌ No choices in response";
            last;
        }

        my $message = $choice<message> // {};
        my $finish  = $choice<finish_reason> // '';

        if $message<content> {
            note "  💬 {$worker-id}: {$message<content>.substr(0, 100)}...";
            $final-content = $message<content>;
        }

        if $finish eq 'tool_calls' || $message<tool_calls> {
            @messages.push: $message;

            my @tcs = $message<tool_calls>.List;
            note "  🔧 {$worker-id} requested {+@tcs} tool call(s)";

            for @tcs -> $tc {
                my $fn    = $tc<function>;
                my $name  = $fn<name>;
                my %args  = try from-json($fn<arguments>) // {};
                my $tc-id = $tc<id> // 'unknown';

                note "    ⚙️ {$name}";
                my %result = exec-tool($nats, $name, $tc-id, %args);
                @messages.push: {
                    :role<tool>,
                    :tool_call_id($tc-id),
                    :content(to-json(%result)),
                };
            }

            note "  🔄 Resending to model...";
            next;
        }

        if $finish eq 'stop' {
            @messages.push: $message;
            note "  ✅ {$worker-id} finished task";
            last;
        }

        note "  ⚠️ finish_reason={$finish}";
        last;
    }

    return $final-content;
}

sub call-model($nats, @messages --> Hash) {
    my $inbox = "_INBOX.wkr." ~ (('a'..'z').pick xx 10).join;
    my $sub   = $nats.subscribe: $inbox;
    my $reply = start await $sub.supply.head.Promise;

    $nats.publish: 'model.deepseek.completion', to-json({
        :id((^2**32).pick.fmt('%08x')),
        :model('deepseek-v4-pro'),
        :@messages,
        :@tools,
        :tool_choice<auto>,
    }), :reply-to($inbox);

    my $msg = await $reply;
    $nats.unsubscribe: $sub;

    return { :error("No response from model") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("JSON parse fail: $!") };
}

sub exec-tool($nats, Str $name, Str $tc-id, %args --> Hash) {
    my $inbox = "_INBOX.tl." ~ (('a'..'z').pick xx 10).join;
    my $sub   = $nats.subscribe: $inbox;
    my $reply = start await $sub.supply.head.Promise;

    $nats.publish: "tools.exec.{$name}", to-json({
        :$name,
        :tool_call_id($tc-id),
        :arguments(%args),
    }), :reply-to($inbox);

    my $msg = await $reply;
    $nats.unsubscribe: $sub;

    return { :error("No response from tool executor") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("JSON parse fail in tool result") };
}
