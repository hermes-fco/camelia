#!/usr/bin/env raku
# 🌺 Camélia PoC #6 — Session Store (persistent session memory)
#
# Isolated container that manages session CRUD via NATS.
# Backend: JetStream stream SESSIONS (one message per session, max 1 per subject).
# All communication via NATS request-reply.
#
# Subjects:
#   session.store.create   → creates new session, returns session_id
#   session.store.get      → retrieves session by id
#   session.store.append   → adds entry to session history
#   session.store.list     → lists all active sessions
#   session.store.delete   → removes a session
#
# Session object stored in JetStream:
#   {
#     session_id: "sess-abc123",
#     created_at: "2026-06-13T...",
#     task_count: 3,
#     history: [
#       {role: "user", content: "..."},
#       {role: "assistant", content: "..."},
#       ...
#     ]
#   }

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

# ── Subscribe to all API subjects ──
my %subs;
for <create get append list delete> -> $op {
    my $subject = "session.store.{$op}";
    %subs{$op} = $nats.subscribe: $subject;
    note "🟢 Listening on {$subject}";
}

# ── Pipe all subscriptions into a channel ──
my $chan = Channel.new;
for %subs.kv -> $op, $sub {
    $sub.supply.tap: -> $msg {
        next unless $msg.payload;
        $chan.send: { :$op, :$msg };
    }
}

note "🟢 Session Store ready.";

# ═════════════════════════════════════════════
# MAIN LOOP
# ═════════════════════════════════════════════

react {
    whenever $chan -> %entry {
        my $op  = %entry<op>;
        my $msg = %entry<msg>;
        my $reply-to = $msg.?reply-to;

        unless $reply-to {
            note "⚠️ session.store.{$op} without reply-to, ignoring";
            next;
        }

        my %req = try from-json($msg.payload);
        if $! {
            $nats.publish: $reply-to, to-json({ :error("Invalid JSON") });
            next;
        }

        given $op {
            when 'create' { handle-create($reply-to, %req) }
            when 'get'    { handle-get($reply-to, %req) }
            when 'append' { handle-append($reply-to, %req) }
            when 'list'   { handle-list($reply-to) }
            when 'delete' { handle-delete($reply-to, %req) }
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
        :max-msgs-per-subject(1),         # only keep latest per session
        :discard<old>,                     # discard old when new arrives
        ;

    my $supply = $stream.create;
    my $resp = await $supply.Promise;
    if $resp && $resp.payload && $resp.payload.contains('"error"') {
        # Stream may already exist — that's fine
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

    my %session = {
        :session_id($sid),
        :created_at($now),
        :task_count(0),
        :history([]),
    };

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
    my $sid      = %req<session_id> // '';
    my $role     = %req<role>       // '';
    my $content  = %req<content>    // '';

    unless $sid && $role && $content {
        $nats.publish: $reply-to, to-json({ :error("Missing session_id, role, or content") });
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

    # Append entry
    %session<history>.push: { :$role, :$content };
    %session<task_count> = (%session<task_count> // 0) + 1;

    # Write back
    my $subject = "session.data.{$sid}";
    $nats.publish: $subject, to-json(%session);

    note "  📝 Appended {$role} to session {$sid} (task_count={%session<task_count>})";
    $nats.publish: $reply-to, to-json({ :ok(True), :session_id($sid), :task_count(%session<task_count>) });
}

sub handle-list(Str $reply-to) {
    # Use stream info to list subjects
    my $info-supply = $stream.info;
    my $info-resp = await $info-supply.Promise;

    unless $info-resp && $info-resp.payload {
        $nats.publish: $reply-to, to-json({ :sessions([]) });
        return;
    }

    my %info = try from-json($info-resp.payload);
    my $num-subjects = %info<state><num_subjects> // 0;

    # Get session IDs — we can get them from stream names or just report count
    # For simplicity, report count. Full list would need stream subject enumeration.
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

    # Purge the specific subject using raw JetStream API
    my $purge-subject = "\$JS.API.STREAM.PURGE.SESSIONS";
    my $purge-body = to-json({ :filter("session.data.{$sid}") });
    $nats.request: $purge-subject, $purge-body;
    note "  🗑️ Deleted session {$sid}";

    $nats.publish: $reply-to, to-json({ :ok(True), :deleted($sid) });
}
