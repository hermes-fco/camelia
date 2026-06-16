#!/usr/bin/env raku
# MESMO script, com CATCH — o react sobrevive
use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222';
my $nats = Nats.new(:servers[$nats-url]);
await $nats.start;
$nats.connect;

note "🟢 Conectado. Enviando 5 mensagens (a 3ª é JSON inválido)...\n";

start {
    sleep 1;
    for 1..5 -> $i {
        my $payload = $i == 3
            ?? '{"isto não é JSON válido!!!'
            !! to-json({ :seq($i), :msg("mensagem {$i}") });
        $nats.publish("demo.react.catch", $payload);
        note "📤 Enviada msg #{$i}" ~ ($i == 3 ?? " (JSON INVÁLIDO)" !! "");
        sleep 1;
    }
    note "\n📤 Publicador terminou.";
}

my $sub = $nats.subscribe('demo.react.catch');
my $received = 0;

react {
    whenever $sub.supply -> $msg {
        CATCH {
            default {
                note "⚠️ Msg ignorada: " ~ .message.split("\n")[0];
                next;
            }
        }
        $received++;
        my %data = from-json($msg.payload);
        note "✅ Recebida msg #{%data<seq>}: {%data<msg>}";
    }
}

note "🏁 React encerrou. Total recebido: {$received}\n";
note "Se $received == 5, o CATCH resolveu o problema.";
