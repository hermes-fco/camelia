#!/usr/bin/env raku
# 🌺 Camelia PoC #2 - Agent with Tool Calling

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222';

# ── Tools schema (what the model can call) ──
my @tools = (
    {
        type     => "function",
        function => {
            name        => "run_shell",
            description => "Executa um comando shell no sandbox Linux e retorna stdout, stderr e exit code.",
            parameters  => {
                type       => "object",
                properties => {
                    command => { type => "string", description => "Comando shell a executar" },
                },
                required => ["command"],
            },
        },
    },
    {
        type     => "function",
        function => {
            name        => "read_file",
            description => "Le um arquivo do sandbox e retorna o conteudo com linhas numeradas.",
            parameters  => {
                type       => "object",
                properties => {
                    path   => { type => "string", description => "Caminho do arquivo (relativo ao sandbox)" },
                    offset => { type => "integer", description => "Linha inicial (0-indexed, default 0)" },
                    limit  => { type => "integer", description => "Maximo de linhas (default 500)" },
                },
                required => ["path"],
            },
        },
    },
    {
        type     => "function",
        function => {
            name        => "write_file",
            description => "Escreve conteudo em um arquivo no sandbox.",
            parameters  => {
                type       => "object",
                properties => {
                    path    => { type => "string", description => "Caminho do arquivo (relativo ao sandbox)" },
                    content => { type => "string", description => "Conteudo a escrever" },
                },
                required => ["path", "content"],
            },
        },
    },
);

# ── System prompt ──
my $system = q:to/END/;
Voce e um assistente de terminal Linux conciso. Pode executar comandos shell,
ler e escrever arquivos no sandbox (/tmp/sandbox).
Sempre responda em portugues brasileiro.
Seja direto e pratico - va direto ao ponto.
END

# ── User prompt ──
my $user-prompt = %*ENV<PROMPT> // 'Liste os arquivos do diretorio atual, depois crie um arquivo chamado "ola.txt" com o texto "Ola Camelia!" e mostre o conteudo dele.';

note "🟡 Conectando NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 NATS conectado.";

# ── Monta histórico inicial ──
my @messages = (
    { :role<system>, :content($system) },
    { :role<user>,   :content($user-prompt) },
);

# ── Loop principal de conversação ──
my $max-turns = 10;
loop {
    last if $max-turns-- <= 0;

    my $request-id = (^2**32).pick.fmt('%08x');

    # Monta request com tools
    my $request = to-json {
        :id($request-id),
        :model('deepseek-v4-pro'),
        :messages(@messages),
        :@tools,
        :tool_choice<auto>,
    };
    note "DEBUG payload: chars={$request.chars} bytes={$request.encode('utf8').bytes}";

    note "📤 Turno {5 - $max-turns}: enviando para o model (id=$request-id)...";

    # Inbox pra resposta do model
    my $model-inbox = "_INBOX.model." ~ (('a'..'z').pick xx 12).join;
    my $model-sub   = $nats.subscribe: $model-inbox;
    # Tap BEFORE publish — avoids race where reply arrives before listener
    my $model-reply = start await $model-sub.supply.head.Promise;

    $nats.publish: 'model.deepseek.completion', $request, :reply-to($model-inbox);

    note "DEBUG aguardando resposta no inbox $model-inbox...";
    my $model-msg = await $model-reply;
    $nats.unsubscribe: $model-sub;
    note "DEBUG resposta recebida! Defined={$model-msg.defined}, Payload={$model-msg.?payload.?chars // 'NONE'}";
    unless $model-msg && $model-msg.payload {
        note "❌ Sem resposta do model";
        last;
    }

    note "DEBUG agent payload: {$model-msg.payload.chars} chars, first 300: {$model-msg.payload.substr(0, 300)}";
    my %response = try from-json($model-msg.payload);
    if $! {
        note "❌ JSON inválido do model: $!";
        last;
    }

    if %response<error> {
        note "❌ Model error: {%response<error>}";
        last;
    }

    my $choice  = %response<choices>[0];
    unless $choice {
        note "❌ No choices in response: {%response.keys}";
        last;
    }
    my $message = $choice<message> // {};
    my $finish  = $choice<finish_reason> // '';

    # Se tem conteudo textual, mostra
    if $message<content> {
        say "🤖 {$message<content>}";
    }

    # Se é tool_call, processa
    if $finish eq 'tool_calls' || $message<tool_calls> {
        # Adiciona a mensagem do assistant ao histórico
        @messages.push: $message;

        my @tool-calls = $message<tool_calls>.List;
        note "🔧 Model pediu {+@tool-calls} tool call(s)";

        # Executa cada tool call em paralelo
        my @results;
        for @tool-calls -> $tc {
            my $fn     = $tc<function>;
            my $name   = $fn<name>;
            my $args   = try from-json($fn<arguments>) // {};
            my $tc-id  = $tc<id> // 'unknown';

            note "  ⚙️ {$name} (id={$tc-id})";

            # Publica no tool-executor com inbox
            my $tool-inbox = "_INBOX.tool." ~ (('a'..'z').pick xx 12).join;
            my $tool-sub   = $nats.subscribe: $tool-inbox;
            my $tool-reply = $tool-sub.supply.head.Promise;

            $nats.publish: "tools.exec.{$name}", to-json({
                :name($name),
                :tool_call_id($tc-id),
                :arguments($args),
            }), :reply-to($tool-inbox);

            my $tool-msg = await $tool-reply;
            $nats.unsubscribe: $tool-sub;
            if $tool-msg && $tool-msg.payload {
                my %result = try from-json($tool-msg.payload);
                @results.push: %result;
                note "  ✅ {$name} done";
            } else {
                @results.push: { :error("No response from tool executor") };
                note "  ❌ {$name} timeout";
            }
        }

        # Adiciona tool results ao histórico
        for @tool-calls Z @results -> ($tc, $result) {
            @messages.push: {
                :role<tool>,
                :tool_call_id($tc<id>),
                :content(to-json($result)),
            };
        }

        # Continua o loop - reenvia pro model com resultados
        note "🔄 Reenviando para o model com resultados...";
        next;
    }

    # finish_reason 'stop' - terminou
    if $finish eq 'stop' {
        # Adiciona a mensagem final ao histórico
        @messages.push: $message;
        note "✅ Conversa finalizada.";
        last;
    }

    # Outros finish_reason (length, content_filter, etc)
    note "⚠️ finish_reason={$finish} - encerrando.";
    last;
}

$nats.stop;
