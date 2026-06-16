#!/usr/bin/env raku
# MESMO script, mas COM try — o react NÃO morre mais
use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222';
my $nats = Nats.new(:servers[$nats-url]);
await $nats.start;
$nats.connect;

note "🟢 Conectado. Enviando 5 mensagens (a 3ª é JSON inválido)...\n";

# Publisher: envia 5 mensagens com intervalo
start {
    sleep 1;
    for 1..5 -> $i {
        my $payload = $i == 3
            ?? '{"isto não é JSON válido!!!'
            !! to-json({ :seq($i), :msg("mensagem {$i}") });
        $nats.publish("demo.react.fixed", $payload);
        note "📤 Enviada msg #{$i}" ~ ($i == 3 ?? " (JSON INVÁLIDO)" !! "");
        sleep 1;
    }
    note "\n📤 Publicador terminou.";
}

# Consumidor com react — COM try (sobrevive à msg #3)
my $sub = $nats.subscribe('demo.react.fixed');
my $received = 0;

react {
    whenever $sub.supply -> $msg {
        $received++;
        # ✅ try captura a exceção, retorna Nil, react continua vivo
        my %data = try from-json($msg.payload) // do {
            note "⚠️ Msg #{$received}: JSON inválido, ignorando.";
            next;
        };
        note "✅ Recebida msg #{%data<seq>}: {%data<msg>}";
    }
}

note "🏁 React encerrou. Total recebido: {$received}\n";
note "Se $received == 5, o try resolveu o problema.";
