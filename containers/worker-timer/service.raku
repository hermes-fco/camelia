#!/usr/bin/env raku
# 🌺 Camélia — Worker Timer (simplified, resilient)
#
# Receives timer requests, persists to JetStream, runs countdown, fires.
# On restart, replays pending timers from stream via direct API.

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url  = %*ENV<NATS_URL>  // 'nats://127.0.0.1:4222';
my $worker-id = ('t' ~ (^10000).pick).Str;

note "🟡 Worker-Timer-{$worker-id} connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 Worker-Timer-{$worker-id} connected.";

# Register in KV so orchestrator knows about this worker type
$nats.publish: '$KV.WORKER_REGISTRY.timer', to-json({
    :name<timer>,
    :subject("worker.timer.task.>"),
    :description("Sets timers and notifies when they fire. Resilient: survives restarts. Args: duration_seconds (Int) or duration_minutes (Num)"),
    :topics([]),
});
note "📋 Registered timer worker in KV registry";

# Subscribe to all timer tasks
my $task-sub = $nats.subscribe: 'worker.timer.>';
note "🟢 Listening on worker.timer.>";

# Health check
my $health-sub = $nats.subscribe: 'health.check.worker.timer';

# Track idle time (for spawner GC)
my $last-activity = now;

# Lifecycle
my $lifecycle-subject = "worker.status.timer.{$worker-id}";
sub lifecycle(Str $event) {
    $nats.publish: "{$lifecycle-subject}.{$event}",
        to-json({ :$worker-id, :type<timer>, :$event, :ts(now.Numeric.Num) });
}

# ═════════════════════════════════════════════
# PERSISTENCE: JetStream stream as KV store
# ═════════════════════════════════════════════

my constant $STORE-STREAM  = 'TIMER_STORE';
my constant $STORE-SUBJECT = 'timer.store.>';

# Set up the store stream (creates if not exists)
sub setup-store() {
    my $stream = Nats::Stream.new:
        :$nats, :name($STORE-STREAM),
        :subjects([$STORE-SUBJECT]),
        :retention<limits>,
        :max-msgs-per-subject(1),
        :discard<new>,
        :allow-direct,
        ;
    my $s = $stream.create;
    await $s.Promise;
    note "✅ TIMER_STORE ready";
}

# ── Countdown + fire ──
sub run-timer(Str $task-id, Str $message, Str $user-reply, Str $user-chat,
              Int $remaining is copy, Str $edit-key) {
    start {
        my $interval = $remaining <= 60 ?? 1 !! 5;

        # Initial countdown message
        $nats.publish: $user-reply, to-json({
            :text("⏱️ *Timer: {$remaining}s restantes* — _{$message}_"),
            :chat_id($user-chat),
            :parse_mode<markdown>,
            :edit_key($edit-key),
        });

        # Countdown loop
        while $remaining > 0 {
            await Promise.in($interval);
            $remaining -= $interval;
            if $remaining <= 0 { $remaining = 0; last; }
            $nats.publish: $user-reply, to-json({
                :text("⏱️ *Timer: {$remaining}s restantes* — _{$message}_"),
                :chat_id($user-chat),
                :parse_mode<markdown>,
                :edit_key($edit-key),
            });
        }

        # Final notification — include current timestamp
        my $now-str = DateTime.now.local.Str.substr(0, 19);
        note "🔔 Timer {$task-id} fired!";
        $nats.publish: $user-reply, to-json({
            :text("🔔 *Timer!* _{$message}_\n\n🕐 *Hora atual:* {$now-str}"),
            :chat_id($user-chat),
            :parse_mode<markdown>,
            :edit_key($edit-key),
        });

        # Clean up KV store — publish empty payload (tombstone)
        $nats.publish: "timer.store.{$task-id}", "";
        lifecycle('idle');
        $last-activity = now;
    }
}

# ═════════════════════════════════════════════
# STARTUP: setup store, then enter react
# ═════════════════════════════════════════════

setup-store();
lifecycle('started');
note "✅ Timer worker ready.";

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

        my $task-id      = %task<id>     // 'unknown';
        my $message      = %task<task>   // 'Timer expired';
        my %args         = %task<args>   // {};
        my $user-reply   = %task<user_reply_to> // '';
        my $user-chat    = %task<user_chat_id>  // '';

        # Calculate duration in seconds
        my $duration-sec = %args<duration_seconds> //
                          (%args<duration_minutes> // 0) * 60;

        # Fallback: extract duration from task text
        if $duration-sec <= 0 {
            if $message ~~ /(\d+) \s* s/ { $duration-sec = +$0 }
            elsif $message ~~ /(\d+\.?\d*) \s* m/ { $duration-sec = (+$0) * 60 }
            elsif $message ~~ /(\d+) \s* (sec|segundo)/ { $duration-sec = +$0 }
            elsif $message ~~ /(\d+) \s* (min|minuto)/ { $duration-sec = (+$0) * 60 }
        }

        if $duration-sec <= 0 {
            note "   ❌ Invalid duration";
            $nats.publish: $reply-to, to-json({
                :ok(False),
                :error("Invalid duration"),
            });
            next;
        }

        $last-activity = now;
        lifecycle('busy');

        my $fire-at  = now.Numeric.Num + $duration-sec;
        my $edit-key = "timer-{$task-id}";

        note "⏱️ Timer {$task-id}: {$duration-sec}s — \"{$message.substr(0, 80)}\"";

        # ── PERSIST to JetStream stream ──
        $nats.publish: "timer.store.{$task-id}", to-json({
            :task_id($task-id),
            :fire_at($fire-at),
            :$message,
            :user_reply_to($user-reply),
            :user_chat_id($user-chat),
            :edit_key($edit-key),
            :created_at(now.Numeric.Num),
        });

        # ── Respond IMMEDIATELY ──
        $nats.publish: $reply-to, to-json({
            :ok(True),
            :worker_id($worker-id),
            :worker_type<timer>,
            :result({
                :timer_set(True),
                :duration_seconds($duration-sec),
                :$message,
            }),
        });

        $last-activity = now;
        lifecycle('idle');

        # ── Background: countdown + fire ──
        if $user-reply && $user-chat {
            run-timer($task-id, $message, $user-reply, $user-chat, $duration-sec, $edit-key);
        } else {
            note "⚠️ Timer {$task-id}: no user_reply_to";
        }
    }

    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service<worker-timer>,
                :$worker-id,
                :idle_seconds((now - $last-activity).Int),
            });
        }
    }
}
