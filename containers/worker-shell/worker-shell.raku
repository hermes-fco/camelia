#!/usr/bin/env raku
# 🌺 Camélia PoC #3 — Worker Shell (typed worker)
#
# Subscribes to worker.shell.> — receives tool execution requests,
# forwards to tool-executor, returns results.
# Direct NATS request-reply, no JetStream for simplicity.

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222';

note "🟡 Worker-Shell connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 Worker-Shell connected.";

# Subscribe to all shell worker tasks
my $task-sub = $nats.subscribe: 'worker.shell.>';
note "🟢 Listening on worker.shell.>";

# Health check
my $health-sub = $nats.subscribe: 'health.check.worker.shell';

# ── Tool execution helper ──
sub exec-tool(Str $name, Str $tc-id, %args --> Hash) {
    my $sub = $nats.subscribe: my $inbox = "_INBOX.sh." ~ (^1_000_000).pick, :1max-messages;
    my $p   = $sub.supply.head.Promise;
    $nats.publish: "tools.exec.{$name}", to-json({
        :$name, :tool_call_id($tc-id), :arguments(%args),
    }), :reply-to($inbox);
    await Promise.anyof: $p, Promise.in(30);
    $nats.unsubscribe: $sub;
    return { :error("Tool timeout") } unless $p.so;
    my $msg = $p.result;
    return { :error("Empty tool response") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("Bad tool JSON") };
}

react {
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
        }
    }

    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service<worker-shell>,
            });
        }
    }
}
