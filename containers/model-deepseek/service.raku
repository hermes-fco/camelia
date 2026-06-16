#!/usr/bin/env raku
# 🌺 Camélia — Model Provider: DeepSeek (core NATS + Channel, no react)
#
# Listens on model.deepseek.completion via core NATS subscription.
# Channel pattern avoids nats.raku multi-sub bug.
# Synchronous processing (one LLM call at a time) keeps it simple.

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $model    = %*ENV<DEEPSEEK_MODEL>    // 'deepseek-v4-pro';
my $nats-url = %*ENV<NATS_URL>          // 'nats://127.0.0.1:4222';

note "🟡 Model DeepSeek connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 NATS connected.";

# Auth header for curl
spurt('/tmp/auth_header', 'Authorization: Bearer ' ~ %*ENV<DEEPSEEK_API_KEY>);

# ── API helper ──
sub deepseek-api(Str $body --> Str) {
    my $proc = Proc::Async.new(
        'curl', '-s',
        '--connect-timeout', '30',
        '--max-time', '120',
        'https://api.deepseek.com/v1/chat/completions',
        '-H', '@/tmp/auth_header',
        '-H', 'Content-Type: application/json',
        '-d', $body,
    );
    my $output = '';
    $proc.stdout.lines(:chomp).tap(-> $line { $output ~= $line ~ "\n" });
    await $proc.start;
    return $output;
}

# ── REACT LOOP ──
my $sub = $nats.subscribe: 'model.deepseek.completion';
note "🔄 Model DeepSeek ready.";

my $last-alive = now.Int;

react {
    whenever $sub.supply -> $msg {
        next unless $msg.payload;

        my $reply-to = $msg.?reply-to;
        unless $reply-to {
            note "⚠️ No reply-to, skipping";
            next;
        }

        # ✅ try protege contra JSON inválido
        my $parsed = try from-json($msg.payload);
        unless $parsed.defined {
            note "⚠️ JSON inválido, ignorando";
            next;
        }
        my %req = $parsed;
        note "📨 Completion request received";

        # Build API body
        my %api-body = %(
            :$model,
            :messages(%req<messages> // []),
        );
        %api-body<tools> = %req<tools> if %req<tools>:exists;
        %api-body<tool_choice> = %req<tool_choice> if %req<tool_choice>:exists;
        my $body = to-json(%api-body);

        # Call DeepSeek with retries
        my $resp = '';
        my $attempts = 3;
        for 1 .. $attempts -> $attempt {
            $resp = deepseek-api($body);
            last if $resp ~~ /^ '{' /;
            if $attempt < $attempts {
                my $delay = 2 ** ($attempt - 1);
                note "  ⚠️ Non-JSON response, retrying in {$delay}s...";
                sleep $delay;
            }
        }

        if $resp !~~ /^ '{' / {
            note "❌ Non-JSON after {$attempts} attempts";
            $nats.publish: $reply-to, to-json({ :error("API failed after {$attempts} retries") });
        } else {
            my $data = try from-json($resp);
            if $! {
                $nats.publish: $reply-to, to-json({ :error("JSON parse failed") });
            } elsif $data<error> {
                note "❌ API error: {$data<error><message>}";
                $nats.publish: $reply-to, to-json({ :error($data<error><message>) });
            } else {
                my $choice = $data<choices>[0];
                my $content = $choice<message><content> // '';
                note "💬 {$content.substr(0, 100)}...";
                $nats.publish: $reply-to, to-json($data);
            }
        }

        my $now = now.Int;
        if $now - $last-alive > 300 {
            note "💚 Model DeepSeek alive";
            $last-alive = $now;
        }
    }
}
