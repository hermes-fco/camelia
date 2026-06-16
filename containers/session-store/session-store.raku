#!/usr/bin/env raku
# 🌺 Camélia — Session Store (react + whenever, com try)
use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;
my $nats-url = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222';

note "🟡 Session Store connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 Session Store connected.";

# Data stream (sessions stored here)
my $data-stream = Nats::Stream.new:
    :$nats, :name<SESSIONS>, :subjects(['session.data.>']),
    :retention<limits>, :max-msgs-per-subject(1), :discard<old>,
    :max-age(604800000000000), :allow-direct;
my $ds = $data-stream.create; await $ds.Promise;
note "✅ Stream SESSIONS ready.";

my $sub = $nats.subscribe: 'session.store.>';
note "🔄 Session Store ready.";

# ═════════════ HANDLERS ═════════════

sub handle-create(Str $reply-to, %req) {
    my $sid = 'sess-' ~ (('a'..'z').pick xx 8).join;
    my $now = DateTime.now.utc.Str;
    my %session = %(:session_id($sid), :created_at($now), :task_count(0), :seq(0), :history([]));
    $nats.publish: "session.data.{$sid}", to-json(%session);
    $nats.publish: $reply-to, to-json({ :ok(True), :session_id($sid), :session(%session) });
    note "  🆕 Created {$sid}";
}

sub handle-get(Str $reply-to, %req) {
    my $sid = %req<session_id> // '';
    unless $sid { $nats.publish: $reply-to, to-json({ :error("Missing session_id") }); return }
    my $supply = $data-stream.get-last-msg("session.data.{$sid}");
    my $p = $supply.Promise;
    await Promise.anyof: $p, Promise.in(3);
    unless $p.so && $p.result && $p.result.payload {
        $nats.publish: $reply-to, to-json({ :error("Session not found: {$sid}") }); return;
    }
    my %session = try from-json($p.result.payload);
    if $! { $nats.publish: $reply-to, to-json({ :error("Corrupt session data") }); return; }
    $nats.publish: $reply-to, to-json({ :ok(True), :session(%session) });
}

sub handle-append(Str $reply-to, %req) {
    my $sid = %req<session_id> // '';
    my $expected-seq = %req<expected_seq> // -1;
    my @entries;
    if %req<entries>:exists { @entries = %req<entries>.List }
    else {
        my $role = %req<role> // ''; my $content = %req<content> // '';
        if $role && $content { @entries = [{ :$role, :$content },] }
    }
    unless $sid && @entries.elems > 0 {
        $nats.publish: $reply-to, to-json({ :error("Missing session_id or entries") }); return;
    }
    my $supply = $data-stream.get-last-msg("session.data.{$sid}");
    my $p = $supply.Promise;
    await Promise.anyof: $p, Promise.in(3);
    unless $p.so && $p.result && $p.result.payload {
        $nats.publish: $reply-to, to-json({ :error("Session not found: {$sid}") }); return;
    }
    my %session = try from-json($p.result.payload);
    if $! { $nats.publish: $reply-to, to-json({ :error("Corrupt session data") }); return; }
    if $expected-seq >= 0 && (%session<seq> // -1) != $expected-seq {
        $nats.publish: $reply-to, to-json({ :error("Seq conflict"), :conflict(True) }); return;
    }
    for @entries -> $entry { %session<history>.push: $entry }
    %session<task_count> += @entries.elems;
    %session<seq> = (%session<seq> // 0) + 1;
    if %session<history>.elems > 40 { %session<history> = %session<history>[*-40 .. *].Array }
    $nats.publish: "session.data.{$sid}", to-json(%session);
    $nats.publish: $reply-to, to-json({ :ok(True), :session_id($sid), :task_count(%session<task_count>), :seq(%session<seq>) });
    note "  📝 Appended {+@entries} to {$sid}";
}

sub handle-list(Str $reply-to) {
    my $info-supply = $data-stream.info;
    my $info-resp = await $info-supply.Promise;
    unless $info-resp && $info-resp.payload { $nats.publish: $reply-to, to-json({ :sessions([]) }); return; }
    my %info = try from-json($info-resp.payload);
    my $num-subjects = %info<state><num_subjects> // 0;
    $nats.publish: $reply-to, to-json({ :ok(True), :session_count($num-subjects) });
}

sub handle-delete(Str $reply-to, %req) {
    my $sid = %req<session_id> // '';
    unless $sid { $nats.publish: $reply-to, to-json({ :error("Missing session_id") }); return }
    my $purge-subject = "\$JS.API.STREAM.PURGE.SESSIONS";
    my $purge-body = to-json({ :filter("session.data.{$sid}") });
    my $purge-supply = $nats.request: $purge-subject, $purge-body;
    await Promise.anyof: $purge-supply.head.Promise, Promise.in(5);
    note "  🗑️ Deleted {$sid}";
    $nats.publish: $reply-to, to-json({ :ok(True), :deleted($sid) });
}

# ═════════════ REACT LOOP ═════════════

my $last-alive = now.Int;
react {
    whenever $sub.supply -> $msg {
        # ✅ try protege contra JSON inválido — react não morre
        my $parsed = try from-json($msg.payload);
        unless $parsed.defined {
            note "⚠️ JSON inválido em {$msg.subject}, ignorando";
            next;
        }
        my $reply-to = $msg.?reply-to;
        unless $reply-to { note "⚠️ No reply-to on {$msg.subject}"; next }

        my %req = $parsed;
        note "📨 Received {$msg.subject}";

        given $msg.subject {
            when /create$/ { handle-create($reply-to, %req) }
            when /get$/    { handle-get($reply-to, %req) }
            when /append$/ { handle-append($reply-to, %req) }
            when /list$/   { handle-list($reply-to) }
            when /delete$/ { handle-delete($reply-to, %req) }
            when /health/  { $nats.publish: $reply-to, to-json({ :status<ok>, :service<session-store> }) }
        }

        my $now = now.Int;
        if $now - $last-alive > 300 { note "💚 Session Store alive"; $last-alive = $now }
    }
}
