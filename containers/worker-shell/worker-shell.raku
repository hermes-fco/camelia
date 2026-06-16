#!/usr/bin/env raku
# 🌺 Camélia — Worker Shell (task-store poll + direct tool execution)
#
# Two modes:
#   1. Task-store: polls task.store.next(worker_type='shell'), processes, updates
#   2. Direct tool: listens on worker.shell.> for internal tool-execution requests
#
# Lifecycle events published to worker.status.shell.<id>.*

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url  = %*ENV<NATS_URL>  // 'nats://127.0.0.1:4222';
my $worker-id = ('s' ~ (^10000).pick).Str;

note "🟡 Worker-Shell-{$worker-id} connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 Worker-Shell-{$worker-id} connected.";

# ── Lifecycle events ──
my $lifecycle-subject = "worker.status.shell.{$worker-id}";
sub lifecycle(Str $event) {
    $nats.publish: "{$lifecycle-subject}.{$event}",
        to-json({ :$worker-id, :type<shell>, :$event, :ts(now.Real) });
}

# ── NATS request-reply helper ──
sub nats-request(Str $subject, Str $payload, Int :$timeout = 30 --> Hash) {
    my $sub = $nats.subscribe: my $inbox = "_INBOX.sh." ~ (^1_000_000).pick, :1max-messages;
    my $p   = $sub.supply.head.Promise;
    $nats.publish: $subject, $payload, :reply-to($inbox);
    await Promise.anyof: $p, Promise.in($timeout);
    $sub.unsubscribe;
    return { :error("No response") } unless $p.so;
    my $msg = $p.result;
    return { :error("Empty response") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("JSON parse fail") };
}

# Register in KV
$nats.publish: '$KV.WORKER_REGISTRY.shell', to-json({
    :name<shell>,
    :subject("worker.shell.task.>"),
    :description("Executes bash commands. Claims from task-store for external tasks, handles direct tool calls internally."),
    :topics([]),
});
note "📋 Registered shell worker in KV registry";

# Health check
my $health-sub = $nats.subscribe: 'health.check.worker.shell';

# Direct tool execution: listen for internal tool calls
my $tool-sub = $nats.subscribe: 'worker.shell.>';

# ── Track idle time (for spawner GC) ──
my $last-activity = now;

# ── Tool execution helper ──
sub exec-tool(Str $name, Str $tc-id, %args --> Hash) {
    my $sub = $nats.subscribe: my $inbox = "_INBOX.shx." ~ (^1_000_000).pick, :1max-messages;
    my $p   = $sub.supply.head.Promise;
    $nats.publish: "tools.exec.{$name}", to-json({
        :$name, :tool_call_id($tc-id), :arguments(%args),
    }), :reply-to($inbox);
    await Promise.anyof: $p, Promise.in(300);
    $sub.unsubscribe;
    return { :error("Tool timeout after 300s") } unless $p.so;
    my $msg = $p.result;
    return { :error("Empty tool response") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("Bad tool JSON") };
}

# ── Handle task from task-store ──
sub handle-task(%task --> Hash) {
    my $desc = %task<description> // '';
    note "📨 Worker-Shell-{$worker-id}: {$desc.substr(0, 80)}...";

    # Extract shell command from description
    my $command = $desc;

    # Try common patterns: 'execute X', 'run X', 'use X', '(use X)', 'ls -la /tmp', etc.
    if $desc ~~ /:i 'execute' \s* "'" (.*?) "'" / {
        $command = $0.Str;
    } elsif $desc ~~ /:i 'execute' \s* '"' (.*?) '"' / {
        $command = $0.Str;
    } elsif $desc ~~ /:i 'run' \s+ (.*) / && $0.Str.chars < 200 {
        $command = $0.Str;
    } elsif $desc ~~ / '(' .*? 'use' \s+ (<-[)]>+) ')' / {
        $command = $0.Str.trim;
    } elsif $desc ~~ /:i 'use' \s+ (<-[.,;(]>+) / {
        $command = $0.Str.trim;
    }

    # If the command still looks like a natural language sentence, try harder
    if $command.chars > 150 && $command ~~ / ( 'ls ' \S+ | 'find ' \S+ | 'cat ' \S+ | 'echo ' \S+ ) / {
        $command = $0.Str;
    }

    note "  🔧 Running: {$command.substr(0, 100)}";
    my %result = exec-tool('run_shell', $worker-id, { :$command });

    if %result<error> {
        return %( :ok(False), :error(%result<error>), :$command );
    }

    my $output = %result<result> // %result<output> // '';
    return %( :ok(True), :$command, :$output, :worker_id($worker-id) );
}

# ═════════════════════════════════════════════
# MAIN: task-store polling loop
# ═════════════════════════════════════════════

note "🔄 Task polling loop ready — worker-shell-{$worker-id}";

start {
    sleep 0.5;
    lifecycle('started');

    loop {
        my %resp = nats-request('task.store.next',
            to-json({ :worker_type('shell') }), :timeout(10));

        unless %resp<ok> && %resp<task> {
            sleep 2;
            next;
        }

        my %task = %resp<task>;
        my $task-id = %task<id> // '';
        note "📨 Worker-Shell-{$worker-id}: claimed {$task-id}";

        $last-activity = now;
        lifecycle('busy');

        try {
            my %result = handle-task(%task);
            my $result-str = to-json(%result);

            nats-request('task.store.update', to-json({
                :id($task-id), :status<completed>, :result($result-str),
            }), :timeout(10));
            note "  ✅ {$task-id} completed";
        }

        CATCH {
            default {
                note "  ❌ Task {$task-id} crashed: {.message}";
                try nats-request('task.store.update', to-json({
                    :id($task-id), :status<failed>, :error_msg(.message),
                }), :timeout(10));
            }
        }

        $last-activity = now;
        lifecycle('idle');
    }
}

react {
    # ── Direct tool execution (internal, no task-store) ──
    whenever $tool-sub.supply -> $msg {
        next unless $msg.payload;
        my $reply-to = $msg.?reply-to;
        unless $reply-to { next }

        my $parsed = try from-json($msg.payload);
        if $! || !$parsed { next }
        my %task = $parsed;

        # Skip ping messages from orchestrator
        next if %task<type> && %task<type> eq 'ping';

        my $tool  = %task<tool>  // '';
        my $tc-id = %task<id>    // 'unknown';
        my %args  = %task<arguments> // {};

        $last-activity = now;
        lifecycle('busy');

        if !$tool && %task<task> {
            note "🔧 Shell worker (NL fallback): {%task<task>.substr(0, 80)}...";
            $tool = 'run_shell';
            %args = :command(%task<task>);
        } elsif $tool {
            note "🔧 Shell worker: {$tool} ({$tc-id})";
        } else {
            $nats.publish: $reply-to, to-json({ :error("Missing 'tool' or 'task' field") });
            lifecycle('idle');
            next;
        }

        start {
            my %tool-resp = exec-tool($tool, $tc-id, %args);
            if %tool-resp<error> {
                $nats.publish: $reply-to, to-json({ :ok(False), :error(%tool-resp<error>) });
            } else {
                my %result = %tool-resp<result> // %tool-resp;
                $nats.publish: $reply-to, to-json({ :ok(True), :%result });
            }

            $last-activity = now;
            lifecycle('idle');
        }
    }

    # ── Health check ──
    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service<worker-shell>,
                :$worker-id,
                :idle_seconds((now - $last-activity).Int),
            });
        }
    }
}
