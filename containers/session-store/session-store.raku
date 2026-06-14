#!/usr/bin/env raku
# 🌺 Camélia PoC #6 — Session Store (persistent session memory)
#
# Isolated container for session CRUD via NATS.
# Backend: JetStream stream SESSIONS (max_msgs_per_subject=1, TTL=7d).
# CAS (compare-and-swap) via seq number prevents lost updates.
#
# Subjects:
#   session.store.create   → returns {session_id, session} with seq=0
#   session.store.get      → returns session with current seq
#   session.store.append   → CAS: checks expected_seq, appends entries[], returns new seq
#   session.store.list     → session count
#   session.store.delete   → purges session
#
# Session object:
#   { session_id, created_at, task_count, seq, history: [...] }

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222';

# ── Connect NATS ──
note "🟡 Session Store connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 Session Store connected.";

# ── JetStream stream setup ──
my $stream = setup-stream();
note "✅ Stream SESSIONS ready.";

# ── Subscribe: one wildcard + health ──
my $main-sub   = $nats.subscribe: 'session.store.>';
my $health-sub = $nats.subscribe: 'health.check.session-store';

# ═════════════════════════════════════════════
# MAIN LOOP — react + whenever (preferred pattern)
# ═════════════════════════════════════════════

react {
    whenever $main-sub.supply -> $msg {
        next unless $msg.payload;
        my $reply-to = $msg.?reply-to;
        unless $reply-to {
            note "⚠️ No reply-to on {$msg.subject}, ignoring";
            next;
        }
        my %req = try from-json($msg.payload);
        if $! {
            $nats.publish: $reply-to, to-json({ :error("Invalid JSON") });
            next;
        }

        # Dispatch by subject suffix
        given $msg.subject {
            when /create$/ { handle-create($reply-to, %req) }
            when /get$/    { handle-get($reply-to, %req) }
            when /append$/ { handle-append($reply-to, %req) }
            when /list$/   { handle-list($reply-to) }
            when /delete$/ { handle-delete($reply-to, %req) }
        }
    }

    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({ :status<ok>, :service<session-store> });
        }
    }
}

# ═════════════════════════════════════════════
# JETSTREAM SETUP
# ═════════════════════════════════════════════

sub setup-stream() {
    my $stream = Nats::Stream.new:
        :$nats,
        :name<SESSIONS>,
        :subjects(['session.data.>']),
        :retention<limits>,
        :max-msgs-per-subject(1),
        :discard<old>,
        :max-age(604800000000000),  # 7 days TTL in nanoseconds
        ;

    my $supply = $stream.create;
    my $resp = await $supply.Promise;
    if $resp && $resp.payload && $resp.payload.contains('"error"') {
        note "  ⚠️ Stream may already exist: {$resp.payload.substr(0, 100)}";
    }

    return $stream;
}

# ═════════════════════════════════════════════
# HANDLERS
# ═════════════════════════════════════════════

sub handle-create(Str $reply-to, %req) {
    my $sid = 'sess-' ~ (('a'..'z').pick xx 8).join;
    my $now = DateTime.now.utc.Str;

    my %session = %(
        :session_id($sid),
        :created_at($now),
        :task_count(0),
        :seq(0),
        :history([]),
    );

    my $subject = "session.data.{$sid}";
    $nats.publish: $subject, to-json(%session);

    note "  🆕 Created session {$sid}";
    $nats.publish: $reply-to, to-json({ :ok(True), :session_id($sid), :session(%session) });
}

sub handle-get(Str $reply-to, %req) {
    my $sid = %req<session_id> // '';
    unless $sid {
        $nats.publish: $reply-to, to-json({ :error("Missing session_id") });
        return;
    }

    my $supply = $stream.get-last-msg("session.data.{$sid}");
    my $resp = await $supply.Promise;

    unless $resp && $resp.payload {
        $nats.publish: $reply-to, to-json({ :error("Session not found: {$sid}") });
        return;
    }

    my %session = try from-json($resp.payload);
    if $! {
        $nats.publish: $reply-to, to-json({ :error("Corrupt session data: $!") });
        return;
    }

    $nats.publish: $reply-to, to-json({ :ok(True), :session(%session) });
}

sub handle-append(Str $reply-to, %req) {
    my $sid          = %req<session_id>   // '';
    my $expected-seq = %req<expected_seq> // -1;

    # Accept either single {role, content} or batch entries[]
    my @entries;
    if %req<entries>:exists {
        @entries = %req<entries>.List;
    } else {
        my $role    = %req<role>    // '';
        my $content = %req<content> // '';
        if $role && $content {
            @entries = [{ :$role, :$content }];
        }
    }

    unless $sid && @entries.elems > 0 {
        $nats.publish: $reply-to, to-json({ :error("Missing session_id or entries") });
        return;
    }

    # Fetch current session
    my $supply = $stream.get-last-msg("session.data.{$sid}");
    my $resp = await $supply.Promise;

    unless $resp && $resp.payload {
        $nats.publish: $reply-to, to-json({ :error("Session not found: {$sid}") });
        return;
    }

    my %session = try from-json($resp.payload);
    if $! {
        $nats.publish: $reply-to, to-json({ :error("Corrupt session data: $!") });
        return;
    }

    # ═══ CAS: compare-and-swap ═══
    if $expected-seq >= 0 && (%session<seq> // -1) != $expected-seq {
        note "  ⚠️ Seq conflict on {$sid}: expected {$expected-seq}, current {%session<seq>}";
        $nats.publish: $reply-to, to-json({
            :error("Seq conflict"),
            :conflict(True),
            :expected_seq($expected-seq),
            :current_seq(%session<seq> // -1),
        });
        return;
    }

    # Append entries
    for @entries -> $entry {
        %session<history>.push: $entry;
    }
    %session<task_count> += @entries.elems;
    %session<seq> = (%session<seq> // 0) + 1;

    # ── Truncate history: keep last 40 entries (20 turns) ──
    if %session<history>.elems > 40 {
        %session<history> = %session<history>[*-40 .. *].Array;
    }

    # Write back
    my $subject = "session.data.{$sid}";
    $nats.publish: $subject, to-json(%session);

    note "  📝 Appended {+@entries} entries to {$sid} (seq={%session<seq>}, tasks={%session<task_count>})";
    $nats.publish: $reply-to, to-json({
        :ok(True),
        :session_id($sid),
        :task_count(%session<task_count>),
        :seq(%session<seq>),
    });
}

sub handle-list(Str $reply-to) {
    my $info-supply = $stream.info;
    my $info-resp = await $info-supply.Promise;

    unless $info-resp && $info-resp.payload {
        $nats.publish: $reply-to, to-json({ :sessions([]) });
        return;
    }

    my %info = try from-json($info-resp.payload);
    my $num-subjects = %info<state><num_subjects> // 0;

    note "  📋 Listed sessions: {$num-subjects} active";
    $nats.publish: $reply-to, to-json({
        :ok(True),
        :session_count($num-subjects),
    });
}

sub handle-delete(Str $reply-to, %req) {
    my $sid = %req<session_id> // '';
    unless $sid {
        $nats.publish: $reply-to, to-json({ :error("Missing session_id") });
        return;
    }

    my $purge-subject = "\$JS.API.STREAM.PURGE.SESSIONS";
    my $purge-body = to-json({ :filter("session.data.{$sid}") });
    $nats.request: $purge-subject, $purge-body;
    note "  🗑️ Deleted session {$sid}";

    $nats.publish: $reply-to, to-json({ :ok(True), :deleted($sid) });
}
