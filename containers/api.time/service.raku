#!/usr/bin/env raku
# 🌺 Camélia — API Worker Template
#
# Template for worker.factory to fill in.
# Placeholders: api.time, Time worker — returns current date/time via shell, {{TOOLS_SCHEMA}}, {{TOOL_LOGIC}}

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

# ═══════════════════════════════════════
# api.time Worker — Time worker — returns current date/time via shell
# ═══════════════════════════════════════

my $nats-url = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222';

# Internal secrets — never exposed in NATS messages
constant API-KEY  = %*ENV<WORKER_API_KEY>  // '';
constant BASE-URL = %*ENV<WORKER_BASE_URL> // '';

note "🟡 api.time connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 api.time connected.";

# Subscribe to typed tasks
my $task-sub = $nats.subscribe: 'time.now.>';
note "🟢 Listening on time.now.>";

# Health check
my $health-sub = $nats.subscribe: 'health.check.api.time';

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

# Tools:
#   time.now   — Returns current local time in ISO 8601 format (shell: date -Iseconds)
#   time.utc   — Returns current UTC time in ISO 8601 format (shell: date -u -Iseconds)

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
        note "📨 api.time: {$action}";

        start {
            my %result = handle-task(%task);
            $nats.publish: $reply-to, to-json(%result);
        }
    }

    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service('api.time'),
            });
        }
    }
}

sub handle-task(%task --> Hash) {
    given %task<action> // '' {
        when 'time.now' {
            my $proc = Proc::Async.new('date', '-Iseconds');
            my $output = '';
            $proc.stdout.lines(:chomp).tap(-> $line { $output ~= $line });
            my $result = await $proc.start;
            if $result.exitcode != 0 {
                return { :ok(False), :error("date command failed with exit code {$result.exitcode}") };
            }
            return { :ok(True), :data($output) };
        }
        when 'time.utc' {
            my $proc = Proc::Async.new('date', '-u', '-Iseconds');
            my $output = '';
            $proc.stdout.lines(:chomp).tap(-> $line { $output ~= $line });
            my $result = await $proc.start;
            if $result.exitcode != 0 {
                return { :ok(False), :error("date command failed with exit code {$result.exitcode}") };
            }
            return { :ok(True), :data($output) };
        }
        default {
            return { :error("No handler for action '{%task<action>}'") };
        }
    }
}