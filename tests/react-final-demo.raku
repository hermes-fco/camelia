#!/usr/bin/env raku
# DEMO FINAL: react + whenever com try — NÃO morre, NÃO loopa
use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222';
my $nats = Nats.new(:servers[$nats-url]);
await $nats.start;
$nats.connect;

note "🟢 Exemplo final: 5 msgs, #3 é JSON inválido\n";

start {
    sleep 1;
    for 1..5 -> $i {
        my $payload = $i == 3
            ?? '{"inválido!!!'
            !! to-json({ :seq($i), :msg("mensagem {$i}") });
        $nats.publish("demo.final", $payload);
        note "📤 #{$i}" ~ ($i == 3 ?? " (INVÁLIDO)" !! "");
        sleep 0.5;
    }
}

my $sub = $nats.subscribe('demo.final');
my $received = 0;

react {
    whenever $sub.supply -> $msg {
        $received++;

        # ✅ try retorna Nil em vez de lançar exceção
        my $parsed = try from-json($msg.payload);
        unless $parsed.defined {
            note "⚠️ #{$received}: JSON inválido — ignorado";
            next;
        }
        my %data = $parsed;
        note "✅ #{$received}: msg {%data<seq>} = {%data<msg>}";
    }
}

note "\n🏁 React encerrou. Recebidas: {$received}/5";
note "{$received == 5 ?? '✅ SUCESSO! React sobreviveu ao JSON inválido.' !! '❌ FALHOU.'}";
