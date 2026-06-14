#!/usr/bin/env raku
# 🌺 Camélia — Model Provider: DeepSeek (Raku)

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $model    = %*ENV<DEEPSEEK_MODEL>    // 'deepseek-v4-pro';
my $nats-url = %*ENV<NATS_URL>          // 'nats://127.0.0.1:4222';

note "🟡 Connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 NATS connected.";

my $auth = "Authorization: Bearer %*ENV<DEEPSEEK_API_KEY>";
spurt('/tmp/auth_header', $auth);

my $sub = $nats.subscribe: 'model.deepseek.completion';
note "🟢 Subscribed, SID={$sub.sid}. Entering react...";

# Health check
my $health-sub = $nats.subscribe: 'health.check.model-deepseek';
note "🩺 Health sub SID={$health-sub.sid}";

react {
    whenever $sub.supply -> $msg {
        note "📨 MSG RECEIVED! payload={$msg.payload.chars} chars";
        next unless $msg.payload;
        my %req = try from-json($msg.payload);
        if $! { note "❌ JSON parse: $!"; next; }
        my $request-id = %req<id> // 'unknown';
        my $reply-to   = $msg.?reply-to;
        note "📨 Prompt received (id=$request-id, reply-to={$reply-to // 'NONE'})";

        # start {} isolates blocking curl from react event loop
        # so health checks remain responsive during API calls
        start {
            my %api-body = %(
                :$model,
                :messages(%req<messages> // []),
            );
            %api-body<tools> = %req<tools> if %req<tools>:exists;
            %api-body<tool_choice> = %req<tool_choice> if %req<tool_choice>:exists;

            my $body = to-json(%api-body);
            spurt('/tmp/body.json', $body);

            my $resp = '';
            my $attempts = 3;
            for 1 .. $attempts -> $attempt {
                note "DEBUG: calling DeepSeek (attempt {$attempt}/{$attempts})...";
                $resp = shell(:out,
                    "curl -s --connect-timeout 30 --max-time 120 " ~
                    "https://api.deepseek.com/v1/chat/completions " ~
                    "-H @/tmp/auth_header " ~
                    "-H 'Content-Type: application/json' " ~
                    "-d @/tmp/body.json"
                ).out.slurp(:close);
                note "DEBUG: API done, len={$resp.chars}";
                last if $resp ~~ /^ '{' /;
                if $attempt < $attempts {
                    my $delay = 2 ** ($attempt - 1);
                    note "  ⚠️ Non-JSON response, retrying in {$delay}s...";
                    sleep $delay;
                }
            }

            unlink('/tmp/body.json') if '/tmp/body.json'.IO.e;

            if $resp !~~ /^ '{' / {
                note "❌ Non-JSON after {$attempts} attempts: {$resp.substr(0, 200)}";
                $nats.publish: $reply-to, to-json({ :error("API failed after {$attempts} retries") }) if $reply-to;
                return;
            }

            my $data = try from-json($resp);
            if $! { note "❌ JSON parse: $!"; return; }
            if $data<error> {
                note "❌ API error: {$data<error><message>}";
                $nats.publish: $reply-to, to-json({ :error($data<error><message>) }) if $reply-to;
                return;
            }

            my $choice  = $data<choices>[0];
            my $content = $choice<message><content> // '';
            my $usage   = $data<usage>;
            my $finish  = $choice<finish_reason> // '';
            my @tool-calls = ($choice<message><tool_calls> // []).List;

            if @tool-calls { note "🔧 Tool calls: {+@tool-calls}" }
            if $content { note "💬 {$content.substr(0, 100)}..." }
            note "✅ finish={$finish}, tokens={$usage<total_tokens> // '?'}";

            if $reply-to {
                $nats.publish: $reply-to, to-json $data;
                note "DEBUG: published reply";
            }
        }
    }

    whenever $health-sub.supply -> $msg {
        my $rt = $msg.?reply-to;
        note "HEALTH CHECK received, reply-to=" ~ ($rt // "NONE");
        if $rt {
            $nats.publish: $rt, to-json({ :status<ok>, :service<model-deepseek> });
        }
    }
}
