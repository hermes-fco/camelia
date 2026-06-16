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

# Reply subscription: orchestrator sends responses here (per-chat routing)
my $reply-sub = $nats.subscribe: 'entry.telegram.response.>';
note "🟢 Listening for replies on entry.telegram.response.>";

# Track session per chat for conversation continuity
my %chat-sessions;  # chat_id → session_id

# 🔒 Secure token relay: chat awaiting token → worker inbox
my %awaiting-token;  # chat_id → worker_token_inbox
my %awaiting-confirm; # chat_id → confirm_inbox (for long-running task confirmation)
my %edit-state;       # edit_key → {chat_id, message_id} for editing progress messages

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
    my $raw = telegram-post('sendMessage', %data);
    return {} unless $raw;
    my %result = try from-json($raw) // {};
    if %result<ok> {
        note "  📤 Sent to {$chat-id}";
        return %result;
    }
    note "  ❌ Send failed {$chat-id}: {%result<description> // $raw.substr(0, 100)}";
    return {};
}

# ── Edit message on Telegram ──
sub edit-telegram(Str $chat-id, Int $message-id, Str $text, :$parse-mode = '') {
    my %data = :chat_id($chat-id), :message_id($message-id), :$text;
    %data<parse_mode> = $parse-mode if $parse-mode;
    my $raw = telegram-post('editMessageText', %data);
    return False unless $raw;
    my %result = try from-json($raw) // {};
    if %result<ok> {
        note "  ✏️ Edited {$chat-id}:{$message-id}";
        return True;
    }
    note "  ❌ Edit failed {$chat-id}:{$message-id} — {%result<description> // $raw.substr(0, 100)}";
    return False;
}

# ── Publish to NATS ──
sub emit(Str $subject, %payload) {
    %payload<source> = $entry-name;
    %payload<ts>     = DateTime.now.Str;
    $nats.publish: $subject, to-json(%payload);
    note "📤 {$subject}: " ~ (%payload.keys.grep({$_ ne 'text'}).join(','));
}

# ── Send typing indicator (keeps updating every 4s until stopped) ──
my %typing-active;  # chat_id → Bool
my %typing-loops;   # chat_id → Promise

sub start-typing(Str $chat-id) {
    %typing-active{$chat-id} = True;
    %typing-loops{$chat-id} = start {
        while %typing-active{$chat-id} {
            telegram-post('sendChatAction', { :chat_id($chat-id), :action<typing> });
            sleep 4;
        }
    };
}

sub stop-typing(Str $chat-id) {
    %typing-active{$chat-id} = False;
    %typing-loops{$chat-id}:delete;
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
                        # Handle both message and edited_message
                        my $msg-key = $update<message>   ?? 'message'   !!
                                      $update<edited_message> ?? 'edited_message' !! Nil;
                        unless $msg-key {
                            note "⚠️ Non-message update: " ~ to-json($update.keys, :!pretty) ~ " → " ~ to-json($update, :!pretty).substr(0, 200);
                            next;
                        }
                        with $update{$msg-key} {
                            my $msg  = $_;
                            my $chat = $msg<chat> // {};
                            my $from = $msg<from> // {};
                            my $text = $msg<text> // $msg<caption> // '';
                            unless $text {
                                note "⚠️ Skipping message without text: " ~ to-json($msg, :!pretty).substr(0, 200);
                                next;
                            }

                            my $cid = $chat<id>.Str;

                            # 🤝 Confirm relay: orchestrator asked user a question → route answer
                            if %awaiting-confirm{$cid} -> $confirm-inbox {
                                note "🤝 Confirm relay: {$cid} → {$confirm-inbox}: {$text.substr(0, 80)}";
                                $nats.publish: $confirm-inbox, $text;
                                %awaiting-confirm{$cid}:delete;
                                start-typing($cid);
                                next;
                            }

                            # 🔒 Token relay: if this chat is awaiting token, relay directly to worker
                            if %awaiting-token{$cid} -> $worker-inbox {
                                note "🔒 Token relay: {$cid} → {$worker-inbox}";
                                $nats.publish: $worker-inbox, $text;
                                %awaiting-token{$cid}:delete;
                                send-telegram($cid, "🔒 Token recebido e encaminhado com segurança.");
                                stop-typing($cid);
                                next;
                            }

                            # 🔄 /rotate <worker> — trigger token rotation
                            if $text ~~ /^ '/' rotate \s+ (\S+) / {
                                my $worker-type = $0.Str;
                                note "🔄 Rotate command: {$worker-type} from {$cid}";
                                $nats.publish: "worker.{$worker-type}.control",
                                    to-json({ :action<rotate>, :chat_id($cid) });
                                send-telegram($cid,
                                    "🔄 Comando de rotação enviado para `{$worker-type}`.\n" ~
                                    "Se o worker estiver ativo, ele vai solicitar o novo token.");
                                stop-typing($cid);
                                next;
                            }

                            # Start typing indicator (keeps refreshing until response)
                            start-typing($cid);

                            my %payload = :prompt($text), :chat_id($cid),
                                          :user_id($from<id>.Str),
                                          :username($from<username> // ''),
                                          :first_name($from<first_name> // ''),
                                          :source($entry-name),
                                          :reply_to('entry.telegram.response.' ~ $cid),
                                          :ts(DateTime.now.Str);

                            # Pass existing session_id for conversation continuity
                            %payload<session_id> = %chat-sessions{$cid}
                                if %chat-sessions{$cid}:exists;

                            $nats.publish: 'orchestrator.task',
                                to-json(%payload),
                                :reply-to('entry.telegram.response'),
                                :ack, :timeout(5);  # JetStream-aware publish
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
        my $parsed = try from-json($msg.payload);
        if $! || !$parsed { note "⚠️ Bad reply JSON"; next }
        if $! { note "⚠️ Bad reply JSON"; next }
        my %resp = $parsed;

        my $chat-id = %resp<chat_id> // '';
        my $text    = %resp<text> // %resp<result> // '';
        my $parse   = %resp<parse_mode> // '';

        unless $chat-id && $text {
            note "⚠️ Reply missing chat_id or text";
            next;
        }

        # Store session_id for conversation continuity
        if %resp<session_id> {
            %chat-sessions{$chat-id} = %resp<session_id>;
            note "  🔗 Session {$chat-id} → {%resp<session_id>}";
        }

        # 🔒 Token request: store awaiting state, next user message goes to worker
        if %resp<token_request> && %resp<worker_token_inbox> {
            %awaiting-token{$chat-id} = %resp<worker_token_inbox>;
            note "  🔒 Token mode ON for {$chat-id} → {%resp<worker_token_inbox>}";
        }

        # 🤝 Confirm request: next user message goes to confirm inbox
        if %resp<confirm_request> && %resp<confirm_inbox> {
            %awaiting-confirm{$chat-id} = %resp<confirm_inbox>;
            note "  🤝 Confirm mode ON for {$chat-id} → {%resp<confirm_inbox>}";
        }

        start {
            stop-typing($chat-id);
            note "  📤 RESPONSE TEXT: {$text.substr(0, 150)}...";

            my $edit-key = %resp<edit_key> // '';
            if $edit-key && %edit-state{$edit-key} {
                # Edit existing message instead of sending new one
                my %st = %edit-state{$edit-key};
                my $ok = edit-telegram($chat-id, %st<message_id>, $text, :parse-mode($parse));
                unless $ok {
                    # Edit failed (message deleted?) — send new and update state
                    my $resp-msg = send-telegram($chat-id, $text, :parse-mode($parse));
                    with $resp-msg<result><message_id> {
                        %edit-state{$edit-key} = %( :chat_id($chat-id), :message_id($_) );
                    }
                }
            } else {
                my $resp-msg = send-telegram($chat-id, $text, :parse-mode($parse));
                if $edit-key {
                    with $resp-msg<result><message_id> {
                        %edit-state{$edit-key} = %( :chat_id($chat-id), :message_id($_) );
                    }
                }
            }
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
