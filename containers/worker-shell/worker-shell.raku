#!/usr/bin/env raku
# 🌺 Camélia PoC #3 — Worker Shell (typed worker)
#
# Subscribes to worker.shell.> — receives tool execution requests,
# forwards to tool-executor, returns results.
# Lifecycle events published to worker.status.shell.<id>.*
# Direct NATS request-reply, JetStream-backed for buffer/spawner visibility.

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

# ── Lifecycle events: published to worker.status.shell.<id>.<event> ──
my $lifecycle-subject = "worker.status.shell.{$worker-id}";
sub lifecycle(Str $event) {
    $nats.publish: "{$lifecycle-subject}.{$event}",
        to-json({ :$worker-id, :type<shell>, :$event, :ts(now.Real) });
}
# Subscribe to all shell worker tasks
my $task-sub = $nats.subscribe: 'worker.shell.>';
note "🟢 Listening on worker.shell.>";

# Register in KV so orchestrator knows about this worker type
$nats.publish: '$KV.WORKER_REGISTRY.shell', to-json({
    :name<shell>,
    :subject("worker.shell.task.>"),
    :description("Executes a SINGLE bash one-liner. Chain with && or ;"),
    :topics([]),
});
note "📋 Registered shell worker in KV registry";

# Health check
my $health-sub = $nats.subscribe: 'health.check.worker.shell';

# ── Track idle time (for spawner GC) ──
my $last-activity = now;

# ── Tool execution helper ──
sub exec-tool(Str $name, Str $tc-id, %args --> Hash) {
    my $sub = $nats.subscribe: my $inbox = "_INBOX.sh." ~ (^1_000_000).pick, :1max-messages;
    my $p   = $sub.supply.head.Promise;
    $nats.publish: "tools.exec.{$name}", to-json({
        :$name, :tool_call_id($tc-id), :arguments(%args),
    }), :reply-to($inbox);
    await Promise.anyof: $p, Promise.in(300);
    $nats.unsubscribe: $sub;
    return { :error("Tool timeout after 300s") } unless $p.so;
    my $msg = $p.result;
    return { :error("Empty tool response") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("Bad tool JSON") };
}

react {
    # ── Publish lifecycle.started + registry after spawner/orchestrator react is tapped ──
    start {
        sleep 0.5;
        lifecycle('started');
    }

    whenever $task-sub.supply -> $msg {
        next unless $msg.payload;
        my $reply-to = $msg.?reply-to;
        unless $reply-to {
            note "⚠️ No reply-to, ignoring";
            next;
        }

        my %task = try from-json($msg.payload);
        if $! {
            $nats.publish: $reply-to, to-json({ :error("Invalid JSON") });
            next;
        }

        my $tool   = %task<tool>   // '';
        my $tc-id  = %task<id>     // 'unknown';
        my %args   = %task<arguments> // {};

        $last-activity = now;
        lifecycle('busy');

        # Natural language fallback: treat 'task' field as a shell command
        if $tool {
            note "🔧 Shell worker: {$tool} ({$tc-id})";
        }
        else {
            my $shell-cmd = %task<task> // '';
            if $shell-cmd {
                note "🔧 Shell worker (NL fallback): {$shell-cmd.substr(0, 80)}...";
                $tool = 'run_shell';
                %args = :command($shell-cmd);
            } else {
                $nats.publish: $reply-to, to-json({ :error("Missing 'tool' or 'task' field") });
                lifecycle('idle');
                next;
            }
        }

        # Execute via tool-executor (async to avoid blocking event loop)
        start {
            my %tool-resp = exec-tool($tool, $tc-id, %args);
            if %tool-resp<error> {
                $nats.publish: $reply-to, to-json({ :ok(False), :error(%tool-resp<error>) });
            } else {
                # Flatten: tool-executor returns { ok, result } or just the result directly
                my %result = %tool-resp<result> // %tool-resp;
                $nats.publish: $reply-to, to-json({ :ok(True), :%result });
            }
            note %tool-resp<error>
                ?? "  ❌ {$tool}: {%tool-resp<error>}"
                !! "  ✅ {$tool} done";

            $last-activity = now;
            lifecycle('idle');
        }
    }

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
