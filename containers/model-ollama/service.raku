#!/usr/bin/env raku
# 🌺 Camélia — Model Ollama (proxies NATS → Ollama HTTP API)
# Listens on model.ollama.<model>.completion (unique per model)

use Nats;
use JSON::Fast;
use HTTP::Tinyish;

$*ERR.out-buffer = False;

my $nats-url   = %*ENV<NATS_URL>   // 'nats://127.0.0.1:4222';
my $ollama-url = %*ENV<OLLAMA_URL>  // 'http://ollama:11434';
my $model      = %*ENV<OLLAMA_MODEL> // 'qwen2.5:3b';
my $safe-model = $model.subst(':', '-', :g);
my $subject    = "model.ollama.{$safe-model}.completion";

note "🟡 Model-Ollama connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 NATS connected. Ollama at {$ollama-url}, model={$model}";

my $http = HTTP::Tinyish.new: :timeout(300);

my $sub = $nats.subscribe: $subject;
note "🟢 Subscribed to {$subject}, SID={$sub.sid}. Entering react...";

react {
    whenever $sub.supply -> $msg {
        next unless $msg.payload;
        my $reply-to = $msg.?reply-to;
        unless $reply-to {
            note "⚠️ No reply-to, ignoring";
            next;
        }

        my $parsed = try from-json($msg.payload);
        if $! || !$parsed {
            note "⚠️ Invalid JSON: {$!.message}";
            $nats.publish: $reply-to, to-json({ :error("Invalid JSON") });
            next;
        }
        my %req = $parsed;

        my @messages = %req<messages>.List;
        note "📨 Prompt received (id={%req<id> // 'unknown'}, reply-to={$reply-to})";

        # Build Ollama request
        my %ollama-body = :$model, :messages(@messages), :stream(False);
        %ollama-body<temperature> = %req<temperature> if %req<temperature>;
        %ollama-body<tools>       = %req<tools>       if %req<tools>;
        %ollama-body<tool_choice> = %req<tool_choice> if %req<tool_choice>;

        note "DEBUG: calling Ollama...";
        my %headers = :Content-Type<application/json>;
        my $resp = $http.post(
            "{$ollama-url}/v1/chat/completions",
            :%headers,
            :content(to-json %ollama-body),
        );

        unless $resp<success> {
            note "❌ Ollama error: status={$resp<status>} body={$resp<content>.substr(0, 200)}";
            $nats.publish: $reply-to, to-json({ :error("Ollama HTTP {$resp<status>}: {$resp<content>.substr(0, 300)}") });
            next;
        }

        my %ollama-resp = try from-json($resp<content>);
        if $! {
            note "❌ Ollama JSON parse fail: {$!.message}";
            $nats.publish: $reply-to, to-json({ :error("Ollama JSON parse fail") });
            next;
        }

        note "💬 Ollama done, tokens={%ollama-resp<usage> // {}}";

        # Translate Ollama response to the format orchestrator/workers expect
        my $choice = %ollama-resp<choices>[0] // {};
        my $finish = $choice<finish_reason> // 'stop';
        my $msg-content = $choice<message> // {};

        # Build choice as a proper Hash — NOT [{:a, :b}] which splits into Pairs
        my %choice-hash = :$finish, :$msg-content;
        my @choices = %choice-hash,;

        my %response = %(
            :@choices,
            :usage(%ollama-resp<usage> // {}),
        );

        $nats.publish: $reply-to, to-json(%response);
        note "✅ reply published";
    }
}
