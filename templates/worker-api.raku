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
# All inter-worker data exchange goes through the TASK-STORE:
#   Orchestrator → task.store.create (subtask) → task-store
#   Worker → task.store.next → processes → task.store.update
#   Orchestrator → polls task-store for completed tasks
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

# Health check
my $health-sub = $nats.subscribe: 'health.check.{{NAME}}';

# ── Track idle time (for spawner GC) ──
my $last-activity = now;

# ── NATS request-reply helper ──
sub nats-request(Str $subject, Str $payload, Int :$timeout = 30 --> Hash) {
    my $sub = $nats.subscribe: my $inbox = "_INBOX.wkr." ~ (^1_000_000).pick, :1max-messages;
    my $p   = $sub.supply.head.Promise;
    $nats.publish: $subject, $payload, :reply-to($inbox);
    await Promise.anyof: $p, Promise.in($timeout);
    $sub.unsubscribe;
    return { :error("No response") } unless $p.so;
    my $msg = $p.result;
    return { :error("Empty response") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("JSON parse fail") };
}

# ── HTTP helper ──
sub http-get(Str $path, :%headers = ()) {
    my @args = ('curl', '-s', '--connect-timeout', '10', BASE-URL ~ $path);
    for %headers.kv -> $k, $v { @args.push: '-H', "{$k}: {$v}" }
    if API-KEY { @args.push: '-H', 'Authorization: Bearer *** ~ API-KEY }

    my $proc = Proc::Async.new(|@args);
    my $output = '';
    $proc.stdout.lines(:chomp).tap(-> $line { $output ~= $line ~ "\n" });
    my $result = await $proc.start;

    return { :error("HTTP exit={$result.exitcode}") } if $result.exitcode != 0;
    try from-json($output) // { :raw($output) };
}

# {{TOOLS_SCHEMA}}

# ── Task polling loop: claim from task-store, process, update ──
start {
    sleep 0.5;  # let react start first
    lifecycle('started');

    loop {
        # Claim next pending task for this worker type
        my %resp = nats-request('task.store.next',
            to-json({ :worker_type('{{NAME}}') }), :timeout(10));

        unless %resp<ok> && %resp<task> {
            # No tasks available — sleep and retry
            sleep 2;
            next;
        }

        my %task = %resp<task>;
        my $task-id = %task<id> // '';
        note "📨 {{NAME}}-{$worker-id}: claimed {$task-id} — {%task<description>.substr(0, 80)}";

        $last-activity = now;
        lifecycle('busy');

        try {
            my %result = handle-task(%task);
            my $result-str = to-json(%result);

            nats-request('task.store.update', to-json({
                :id($task-id), :status<completed>, :result($result-str),
            }), :timeout(10));
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
    # ── Health check (with idle_seconds for spawner GC) ──
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
    return { :error("No handler for action '{%task<action>}'\nTask: {%task<description>}") };
}
