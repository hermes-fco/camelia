#!/usr/bin/env raku
# 🌺 Camélia — API Worker Template
#
# Template for worker.factory to fill in.
# Placeholders: {{NAME}}, {{DESCRIPTION}}, {{TOOLS_SCHEMA}}, {{TOOL_LOGIC}}
#
# ═══════════════════════════════════════════════════════════
# 🔒 ISOLATION CONSTRAINT — NO SHARED FILESYSTEM
#
# Workers run in isolated containers. They MUST NOT:
#   • Read files another worker wrote
#   • Write files for another worker to consume
#   • Assume /shared or any cross-container volume exists
#
# All inter-worker data exchange goes through NATS:
#   Worker A → NATS result message → Orchestrator → NATS task message → Worker B
#
# For large payloads, use NATS Object Store (not files).
# /tmp is local and volatile — assume it's wiped on container death.
# ═══════════════════════════════════════════════════════════

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

# ═══════════════════════════════════════
# {{NAME}} Worker — {{DESCRIPTION}}
# ═══════════════════════════════════════

my $nats-url  = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222';
my $worker-id = ('f' ~ (^10000).pick).Str;

# Internal secrets — never exposed in NATS messages
constant API-KEY  = %*ENV<WORKER_API_KEY>  // '';
constant BASE-URL = %*ENV<WORKER_BASE_URL> // '{{BASE_URL}}';

note "🟡 {{NAME}}-{$worker-id} connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 {{NAME}}-{$worker-id} connected.";

# ── Lifecycle events: published to worker.status.{{NAME}}.<id>.<event> ──
my $lifecycle-subject = "worker.status.{{NAME}}.{$worker-id}";
sub lifecycle(Str $event) {
    $nats.publish: "{$lifecycle-subject}.{$event}",
        to-json({ :$worker-id, :type('{{NAME}}'), :$event, :ts(now.Real) });
}
# Subscribe to typed tasks
my $task-sub = $nats.subscribe: 'worker.{{NAME}}.task.>';
note "🟢 Listening on worker.{{NAME}}.task.>";

# Health check
my $health-sub = $nats.subscribe: 'health.check.{{NAME}}';

# ── Track idle time (for spawner GC) ──
my $last-activity = now;

# ── HTTP helper ──
sub http-get(Str $path, :%headers = ()) {
    my @args = ('curl', '-s', '--connect-timeout', '10', BASE-URL ~ $path);
    for %headers.kv -> $k, $v { @args.push: '-H', "{$k}: {$v}" }
    if API-KEY { @args.push: '-H', 'Authorization: Bearer ' ~ API-KEY }

    my $proc = Proc::Async.new(|@args);
    my $output = '';
    $proc.stdout.lines(:chomp).tap(-> $line { $output ~= $line ~ "\n" });
    my $result = await $proc.start;

    return { :error("HTTP exit={$result.exitcode}") } if $result.exitcode != 0;
    try from-json($output) // { :raw($output) };
}

# {{TOOLS_SCHEMA}}

react {
    # ── Publish lifecycle.started after spawner's react is tapped ──
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

        my $action = %task<action> // '';
        note "📨 {{NAME}}-{$worker-id}: {$action}";

        $last-activity = now;
        lifecycle('busy');

        start {
            my %result = handle-task(%task);
            $nats.publish: $reply-to, to-json(%result);

            $last-activity = now;
            lifecycle('idle');
        }
    }

    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service('{{NAME}}'),
                :$worker-id,
                :idle_seconds((now - $last-activity).Int),
            });
        }
    }
}

sub handle-task(%task --> Hash) {
    # {{TOOL_LOGIC}}
    return { :error("No handler for action '{%task<action>}'") };
}
