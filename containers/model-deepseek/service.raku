#!/usr/bin/env raku
# 🌺 Camélia — Model Provider: DeepSeek

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $model    = %*ENV<DEEPSEEK_MODEL>    // 'deepseek-v4-pro';
my $nats-url = %*ENV<NATS_URL>          // 'nats://127.0.0.1:4222';

note "🟡 Conectando NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 NATS conectado.";

my $sub = $nats.subscribe: 'model.deepseek.completion';
note "🟢 Aguardando prompts em model.deepseek.completion...";

# Channel to decouple NATS receive from HTTP calls
my $chan = Channel.new;

# Tap pushes messages to channel (non-blocking, runs on scheduler thread)
$sub.supply.tap: -> $msg {
    $chan.send($msg) if $msg.payload;
}

# Process channel in main thread (blocking-safe for shell calls)
loop {
    my $msg = $chan.receive;
    next unless $msg.payload;

    my %req = try from-json($msg.payload);
    if $! { note "❌ JSON parse: $!"; next; }
    my $request-id = %req<id> // 'unknown';
    my $reply-to   = $msg.reply-to;
    note "📨 Prompt recebido (id=$request-id)";

    # Build API request body
    my %api-body = (
        model    => $model,
        messages => %req<messages> // [],
    );
    %api-body<tools> = %req<tools> if %req<tools>:exists;
    %api-body<tool_choice> = %req<tool_choice> if %req<tool_choice>:exists;

    my $body = to-json(%api-body);
    spurt('/tmp/body.json', $body);

    note "DEBUG: calling DeepSeek (body_len={$body.chars})...";
    my $auth-value = "Bearer " ~ %*ENV<DEEPSEEK_API_KEY>;

    my $resp = shell(:out,
        "curl -s --connect-timeout 30 --max-time 120 " ~
        "https://api.deepseek.com/v1/chat/completions " ~
        "-H 'Authorization: {$auth-value}' " ~
        "-H 'Content-Type: application/json' " ~
        "-d @/tmp/body.json"
    ).out.slurp(:close);
    note "DEBUG: API done, len={$resp.chars}";

    if $resp !~~ /^ '{' / {
        note "❌ Non-JSON: {$resp.substr(0, 200)}";
        $nats.publish: $reply-to, to-json({ :error("Bad response") }) if $reply-to;
        next;
    }

    my $data = try from-json($resp);
    if $! { note "❌ JSON parse from response: $!"; next; }

    if $data<error> {
        note "❌ API error: {$data<error><message>}";
        $nats.publish: $reply-to, to-json({ :error($data<error><message>) }) if $reply-to;
        next;
    }

    my $choice  = $data<choices>[0];
    my $content = $choice<message><content> // '';
    my $usage   = $data<usage>;
    my $finish  = $choice<finish_reason> // '';
    note "✅ Resposta pronta. finish={$finish}, tokens={$usage<total_tokens> // '?'}";

    if $reply-to {
        $nats.publish: $reply-to, to-json($data);
        note "DEBUG: published";
    } else {
        note "❌ No reply-to!";
    }
}
