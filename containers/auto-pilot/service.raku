#!/usr/bin/env raku
# 🌺 Camélia PoC #8 — Auto-Pilot (autonomous supervisor)
#
# Watches session events and periodically checks for stuck/stale sessions.
# When a session's last message is from the user and >5min old with no response,
# it re-submits the prompt to the orchestrator.
#
# Flow:
#   1. Subscribe to session.events → maintain active-session cache
#   2. Every CHECK_INTERVAL seconds, fetch each session via session.store.get
#   3. If last history entry is role=user → re-submit to orchestrator.task

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url       = %*ENV<NATS_URL>       // 'nats://127.0.0.1:4222';
my $check-interval = %*ENV<CHECK_INTERVAL>  // 60;   # seconds between scans
my $stale-seconds  = %*ENV<STALE_SECONDS>   // 300;  # 5 min before re-submit

# ── Connect NATS ──
note "🟡 Auto-Pilot connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 Auto-Pilot connected. Check interval: {$check-interval}s, stale threshold: {$stale-seconds}s";

# ── In-memory cache: session_id → {chat_id, last_role, last_seen} ──
my %sessions;

# ── Subscriptions ──
my $events-sub  = $nats.subscribe: 'session.events';
my $health-sub  = $nats.subscribe: 'health.check.auto-pilot';

note "🟢 Listening on session.events...";

react {
    # ── Event stream: keep local cache updated ──
    whenever $events-sub.supply -> $msg {
        next unless $msg.payload;
        my $ev-parsed = try from-json($msg.payload);
        next if $! || !$ev-parsed;
        my %ev = $ev-parsed;
        my $sid     = %ev<session_id> // '';
        my $role    = %ev<last_role>  // '';
        my $chat-id = %ev<chat_id>    // '';
        next unless $sid;

        %sessions{$sid} = {
            :$chat-id,
            :last_role($role),
            :last_seen(now.Int),
        };
    }

    # ── Periodic scan for stale sessions ──
    whenever Supply.interval($check-interval.Int) {
        my $now = now.Int;
        note "🔍 Auto-Pilot scan: {%sessions.elems} active sessions...";

        my ($checked, $resubmitted) = (0, 0);

        for %sessions.kv -> $sid, $info {
            $checked++;

            # Only check sessions where last event had role=user
            next unless $info<last_role> eq 'user' || $info<last_role> eq '';

            # Fetch full session from session-store
            my $reply-to = "_INBOX.ap." ~ (('a'..'z').pick xx 8).join;
            my $sub      = $nats.subscribe: $reply-to, :1max-messages;
            my $p        = $sub.supply.head.Promise;
            $nats.publish: 'session.store.get',
                to-json({ :session_id($sid) }),
                :reply-to($reply-to);

            await Promise.anyof: $p, Promise.in(5);
            $nats.unsubscribe: $sub;
            next unless $p.so;

            my $resp-msg = $p.result;
            next unless $resp-msg && $resp-msg.payload;
            my $resp-parsed = try from-json($resp-msg.payload);
            next if $! || !$resp-parsed;
            my %resp = $resp-parsed;
            next unless %resp<ok> && %resp<session>;

            my %session = %resp<session>;
            my @history = %session<history>.List;
            next unless @history.elems > 0;

            # Check if last entry is from user
            my $last = @history[*-1];
            next unless $last<role> eq 'user';

            # Check staleness: last seen > stale threshold
            my $age = $now - ($info<last_seen> // 0);
            next unless $age >= $stale-seconds.Int;

            # Re-submit to orchestrator
            my $prompt  = $last<content> // '';
            my $chat-id = $info<chat_id> // '';
            next unless $prompt;

            note "  ⏰ Stale session {$sid}: last user msg {$age}s ago — re-submitting...";

            my $resub-reply = "_INBOX.apr." ~ (('a'..'z').pick xx 8).join;
            my $resub-sub   = $nats.subscribe: $resub-reply, :1max-messages;
            my $rp          = $resub-sub.supply.head.Promise;
            $nats.publish: 'orchestrator.task',
                to-json({ :$prompt, :session_id($sid), :$chat-id }),
                :reply-to($resub-reply);
            await Promise.anyof: $rp, Promise.in(10);
            $nats.unsubscribe: $resub-sub;

            $resubmitted++;
            note "  ✅ Re-submitted {$sid} to orchestrator";
        }

        if $checked > 0 || $resubmitted > 0 {
            note "🔍 Scan done: {$checked} checked, {$resubmitted} resubmitted";
        }

        # ── Task-store: check for stale tasks ──
        try {
            my $ts-reply = "_INBOX.apts." ~ (('a'..'z').pick xx 8).join;
            my $ts-sub   = $nats.subscribe: $ts-reply, :1max-messages;
            my $tp       = $ts-sub.supply.head.Promise;
            $nats.publish: 'task.store.list',
                to-json({ :status<assigned> }),
                :reply-to($ts-reply);
            await Promise.anyof: $tp, Promise.in(5);
            $ts-sub.unsubscribe;

            if $tp.so && $tp.result.payload {
                my $ts-parsed = try from-json($tp.result.payload);
                if !$! && $ts-parsed && $ts-parsed<ok> {
                    my @tasks = $ts-parsed<tasks>.List;
                    for @tasks -> $task {
                        my $updated = $task<updated_at> // '';
                        # Check if task is assigned but not updated for >10 min
                        if $updated {
                            # Simple check: if there are assigned tasks, note them
                            note "  📋 Stale task: {$task<id>} ({$task<description>.substr(0,50)})";
                        }
                    }
                    if @tasks.elems > 0 {
                        note "  📊 {@tasks.elems} assigned task(s) in queue";
                    }
                }
            }
            CATCH { note "  ⚠️ task-store check failed: {.message}" }
        }
    }

    # ── Health check ──
    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service<auto-pilot>,
                :active_sessions(%sessions.elems),
                :check_interval($check-interval.Int),
                :stale_seconds($stale-seconds.Int),
            });
        }
    }
}
