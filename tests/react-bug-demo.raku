#!/usr/bin/env raku
# Reprodução do bug: react + whenever morre silenciosamente
# quando uma exceção não tratada ocorre dentro do whenever.
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
            ?? '{"isto não é JSON válido!!!'  # ← vai quebrar o from-json
            !! to-json({ :seq($i), :msg("mensagem {$i}") });
        $nats.publish("demo.react.bug", $payload);
        note "📤 Enviada msg #{$i}" ~ ($i == 3 ?? " (JSON INVÁLIDO)" !! "");
        sleep 1;
    }
    note "\n📤 Publicador terminou.";
}

# Consumidor com react — SEM CATCH (vai morrer na msg #3)
my $sub = $nats.subscribe('demo.react.bug');
my $received = 0;

react {
    whenever $sub.supply -> $msg {
        next unless $msg.payload;
        $received++;
        my %data = from-json($msg.payload);  # 💥 LINHA DO CRASH
        note "✅ Recebida msg #{%data<seq>}: {%data<msg>}";
    }
}

# Esta linha NUNCA será alcançada se o react morrer
note "🏁 React encerrou. Total recebido: {$received}\n";
note "Se $received < 5, o react morreu na msg #3 e perdeu as msgs #4 e #5.";
