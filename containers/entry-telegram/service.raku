#!/usr/bin/env raku
# 🌺 Camélia — Entry Point: Telegram Bot
#
# Long-polls Telegram API, forwards messages into NATS.
# Subscribes to entry.telegram.response for replies.

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url    = %*ENV<NATS_URL>    // 'nats://127.0.0.1:4222';
my $entry-name  = %*ENV<ENTRY_NAME>  // 'entry.telegram';
my $entry-token = %*ENV<ENTRY_TOKEN> // '';
die "ENTRY_TOKEN (Telegram bot token) is required" unless $entry-token;

my $api-base = "https://api.telegram.org/bot{$entry-token}";
my $poll-interval = (%*ENV<POLL_INTERVAL> // 2).Int;

note "🟡 entry.telegram connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 entry.telegram connected.";

# Health check
my $health-sub = $nats.subscribe: 'health.check.entry.telegram';

# Reply subscription: orchestrator sends responses here
my $reply-sub = $nats.subscribe: 'entry.telegram.response';
note "🟢 Listening for replies on entry.telegram.response";

# ── Telegram API helpers (all Proc::Async) ──
sub telegram-get(Str $method, Str $params = '') {
    my $url = "{$api-base}/{$method}" ~ ($params ?? "?{$params}" !! '');
    my $proc = Proc::Async.new('curl', '-s', '--connect-timeout', '10', '--max-time', '30', $url);
    my $out = '';
    $proc.stdout.lines(:chomp).tap(-> $l { $out ~= $l });
    my $r = await $proc.start;
    return '' if $r.exitcode != 0;
    return $out;
}

sub telegram-post(Str $method, %data) {
    my $url = "{$api-base}/{$method}";
    my $body = to-json(%data);
    my $proc = Proc::Async.new(
        'curl', '-s', '--connect-timeout', '10', '--max-time', '15',
        '-H', 'Content-Type: application/json',
        '-d', $body, $url
    );
    my $out = '';
    $proc.stdout.lines(:chomp).tap(-> $l { $out ~= $l });
    my $r = await $proc.start;
    return '' if $r.exitcode != 0;
    return $out;
}

# ── Send message back to Telegram ──
sub send-telegram(Str $chat-id, Str $text, :$parse-mode = '') {
    my %data = :chat_id($chat-id), :$text;
    %data<parse_mode> = $parse-mode if $parse-mode;
    my $resp = telegram-post('sendMessage', %data);
    note $resp ?? "  📤 Sent to {$chat-id}" !! "  ❌ Failed to send to {$chat-id}";
    return $resp;
}

# ── Publish to NATS ──
sub emit(Str $subject, %payload) {
    %payload<source> = $entry-name;
    %payload<ts>     = DateTime.now.Str;
    $nats.publish: $subject, to-json(%payload);
    note "📤 {$subject}: " ~ (%payload.keys.grep({$_ ne 'text'}).join(','));
}

# ── Long-poll loop ──
my $offset = 0;
my $poll-supply = Supplier.new;

start {
    note "🔁 Starting Telegram poll loop (interval={$poll-interval}s)...";
    loop {
        my $resp = telegram-get('getUpdates',
            "offset={$offset}&timeout=10");

        if $resp && $resp ne '' {
            my $data = try from-json($resp);
            unless $! {
                note "DEBUG: got response ok={$data<ok>}, results=" ~ ($data<result> ?? $data<result>.elems !! 'none');
                if $data<ok> && $data<result> -> @updates {
                    for @updates -> $update {
                        $offset = $update<update_id> + 1;
                        with $update<message> {
                            my $msg  = $_;
                            my $chat = $msg<chat> // {};
                            my $from = $msg<from> // {};
                            my $text = $msg<text> // $msg<caption> // '';
                            next unless $text;

                            $nats.publish: 'orchestrator.task',
                                to-json({
                                    :prompt($text),
                                    :chat_id($chat<id>.Str),
                                    :user_id($from<id>.Str),
                                    :username($from<username> // ''),
                                    :first_name($from<first_name> // ''),
                                    :source($entry-name),
                                    :ts(DateTime.now.Str),
                                }),
                                :reply-to('entry.telegram.response');
                        }
                    }
                }
            }
        }
        sleep $poll-interval;
    }
}

# ── React loop (reply handling + health) ──
react {
    whenever $reply-sub.supply -> $msg {
        next unless $msg.payload;
        my %resp = try from-json($msg.payload);
        if $! { note "⚠️ Bad reply JSON"; next }

        my $chat-id = %resp<chat_id> // '';
        my $text    = %resp<text> // %resp<result> // '';
        my $parse   = %resp<parse_mode> // '';

        unless $chat-id && $text {
            note "⚠️ Reply missing chat_id or text";
            next;
        }

        start {
            send-telegram($chat-id, $text, :parse-mode($parse));
        }
    }

    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service<entry.telegram>,
                :protocol<telegram>,
                :offset($offset),
            });
        }
    }
}
