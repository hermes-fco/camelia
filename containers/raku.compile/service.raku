#!/usr/bin/env raku
# 🌺 Camélia — API Worker Template
#
# Template for worker.factory to fill in.
# Placeholders: raku.compile, Raku compilation tester — validates Raku code via raku -c, {{TOOLS_SCHEMA}}, {{TOOL_LOGIC}}

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

# ═══════════════════════════════════════
# raku.compile Worker — Raku compilation tester — validates Raku code via raku -c
# ═══════════════════════════════════════

my $nats-url = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222';

# Internal secrets — never exposed in NATS messages
constant API-KEY  = %*ENV<WORKER_API_KEY>  // '';
constant BASE-URL = %*ENV<WORKER_BASE_URL> // '';

note "🟡 raku.compile connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 raku.compile connected.";

# Subscribe to typed tasks
my $task-sub = $nats.subscribe: 'worker.raku-compile.task.>';
note "🟢 Listening on raku.compile.>";

# Health check
my $health-sub = $nats.subscribe: 'health.check.raku.compile';

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

# Actions: compile — Validate Raku source code.
#   Params: code (string) — the Raku source to compile.
#   Returns: { ok: true, message: "Syntax OK" } on success,
#             { ok: false, error: "<compiler message>" } on failure.

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
        note "📨 raku.compile: {$action}";

        start {
            my %result = handle-task(%task);
            $nats.publish: $reply-to, to-json(%result);
        }
    }

    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service('raku.compile'),
            });
        }
    }
}

sub handle-task(%task --> Hash) {
    given %task<action> {
        when 'compile' {
            my $code = %task<code>;
            return { :error("Missing 'code' parameter") } unless $code.defined && $code ne '';

            my $tempname = "/tmp/raku-compile-{(^100000).pick}.raku";
            spurt($tempname, $code);

            my $proc = Proc::Async.new('raku', '-c', $tempname);
            my $stdout = '';
            my $stderr = '';
            $proc.stdout.lines(:chomp).tap(-> $line { $stdout ~= "$line\n" });
            $proc.stderr.lines(:chomp).tap(-> $line { $stderr ~= "$line\n" });

            my $result = await $proc.start;
            unlink $tempname;

            # raku -c prints "Syntax OK" on stdout and exits 0 on success.
            if $result.exitcode == 0 && $stdout.contains('Syntax OK') {
                return { :ok(True), :message("Syntax OK") };
            }
            else {
                my $err = $stderr || $stdout || 'Unknown compilation error';
                return { :ok(False), :error($err.trim) };
            }
        }
        default {
            return { :error("No handler for action '{%task<action>}'") };
        }
    }
}