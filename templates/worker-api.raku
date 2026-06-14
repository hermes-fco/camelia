#!/usr/bin/env raku
# 🌺 Camélia — API Worker Template
#
# Template for worker.factory to fill in.
# Placeholders: {{NAME}}, {{DESCRIPTION}}, {{TOOLS_SCHEMA}}, {{TOOL_LOGIC}}

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

# ═══════════════════════════════════════
# {{NAME}} Worker — {{DESCRIPTION}}
# ═══════════════════════════════════════

my $nats-url = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222';

# Internal secrets — never exposed in NATS messages
constant API-KEY  = %*ENV<WORKER_API_KEY>  // '';
constant BASE-URL = %*ENV<WORKER_BASE_URL> // '{{BASE_URL}}';

note "🟡 {{NAME}} connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 {{NAME}} connected.";

# Subscribe to typed tasks
my $task-sub = $nats.subscribe: '{{SUBJECT}}';
note "🟢 Listening on {{SUBJECT}}";

# Health check
my $health-sub = $nats.subscribe: 'health.check.{{NAME}}';

# ── HTTP helper ──
sub http-get(Str $path, :%headers = ()) {
    my @args = ('curl', '-s', '--connect-timeout', '10', BASE-URL ~ $path);
    for %headers.kv -> $k, $v { @args.push: '-H', "{$k}: {$v}" }
    if API-KEY { @args.push: '-H', "Authorization: " ~ "Bearer " ~ API-KEY }

    my $proc = Proc::Async.new(|@args);
    my $output = '';
    $proc.stdout.lines(:chomp).tap(-> $line { $output ~= $line ~ "\n" });
    my $result = await $proc.start;

    return { :error("HTTP exit={$result.exitcode}") } if $result.exitcode != 0;
    try from-json($output) // { :raw($output) };
}

# {{TOOLS_SCHEMA}}

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

        my $action = %task<action> // '';
        note "📨 {{NAME}}: {$action}";

        start {
            my %result = handle-task(%task);
            $nats.publish: $reply-to, to-json(%result);
        }
    }

    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service('{{NAME}}'),
            });
        }
    }
}

sub handle-task(%task --> Hash) {
    # {{TOOL_LOGIC}}
    return { :error("No handler for action '{%task<action>}'") };
}
