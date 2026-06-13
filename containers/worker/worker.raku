#!/usr/bin/env raku
# 🌺 Camélia PoC #3 — Worker Agent (long-running)
#
# Subscribes to worker.<WORKER_ID>.task, processes each task
# through the model+tool loop, returns final result via inbox.

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url  = %*ENV<NATS_URL>  // 'nats://127.0.0.1:4222';
my $worker-id = %*ENV<WORKER_ID> // 'default';
my $subject   = "worker.{$worker-id}.task";

# ── Tools schema (same as agent) ──
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
                    path   => { type => "string",  description => "Caminho do arquivo (relativo ao sandbox)" },
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
Voce e um worker agent especializado. Complete a tarefa usando as ferramentas disponiveis.
Seja minucioso e preciso. Entregue um resultado completo — nao deixe trabalho pela metade.
Sempre responda em portugues brasileiro.
END

note "🟡 Worker {$worker-id} conectando NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 Worker {$worker-id} pronto, aguardando tarefas em {$subject}...";

my $task-sub = $nats.subscribe: $subject;

# ── Helper: call model ──
sub call-model(@messages --> Hash) {
    my $request-id = (^2**32).pick.fmt('%08x');
    my $request = to-json {
        :id($request-id),
        :model('deepseek-v4-pro'),
        :@messages,
        :@tools,
        :tool_choice<auto>,
    };

    my $inbox = "_INBOX.wkr." ~ (('a'..'z').pick xx 10).join;
    my $sub   = $nats.subscribe: $inbox;
    my $reply = start await $sub.supply.head.Promise;

    $nats.publish: 'model.deepseek.completion', $request, :reply-to($inbox);

    my $msg = await $reply;
    $nats.unsubscribe: $sub;

    return { :error("No response from model") } unless $msg && $msg.payload;

    my %resp = try from-json($msg.payload);
    return { :error("JSON parse fail: $!") } if $!;
    return %resp if %resp<error>;
    return %resp;
}

# ── Helper: execute a tool call ──
sub exec-tool(Str $name, Str $tc-id, %args --> Hash) {
    my $inbox = "_INBOX.tl." ~ (('a'..'z').pick xx 10).join;
    my $sub   = $nats.subscribe: $inbox;
    my $reply = start await $sub.supply.head.Promise;

    $nats.publish: "tools.exec.{$name}", to-json({
        :name($name),
        :tool_call_id($tc-id),
        :arguments(%args),
    }), :reply-to($inbox);

    my $msg = await $reply;
    $nats.unsubscribe: $sub;

    return { :error("No response from tool executor") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("JSON parse fail in tool result") };
}

# ── React loop: process tasks as they arrive ──
react {
    whenever $task-sub.supply -> $msg {
        next unless $msg.payload;
        my $reply-to = $msg.?reply-to;
        unless $reply-to {
            note "⚠️ No reply-to in task message, ignorando";
            next;
        }

        my %req = try from-json($msg.payload);
        if $! {
            $nats.publish: $reply-to, to-json({ :error("Invalid JSON") });
            next;
        }

        my $task = %req<task> // 'Execute a task';
        my $role = %req<role> // 'worker';
        note "📨 {$worker-id} recebeu tarefa: {$role} — {$task.substr(0, 100)}...";

        # Build initial messages
        my @messages = (
            { :role<system>, :content($system ~ "\n\nSeu papel nesta tarefa: {$role}") },
            { :role<user>,   :content($task) },
        );

        my $final-content = '';
        my $max-turns = 8;

        loop {
            last if $max-turns-- <= 0;

            my %resp = call-model(@messages);
            if %resp<error> {
                note "  ❌ Model error: {%resp<error>}";
                $final-content = "ERROR: {%resp<error>}";
                last;
            }

            my $choice  = %resp<choices>[0];
            unless $choice {
                note "  ❌ No choices in response";
                last;
            }

            my $message = $choice<message> // {};
            my $finish  = $choice<finish_reason> // '';

            if $message<content> {
                note "  💬 {$worker-id}: {$message<content>.substr(0, 100)}...";
                $final-content = $message<content>;
            }

            if $finish eq 'tool_calls' || $message<tool_calls> {
                @messages.push: $message;

                my @tcs = $message<tool_calls>.List;
                note "  🔧 {$worker-id} pediu {+@tcs} tool call(s)";

                for @tcs -> $tc {
                    my $fn    = $tc<function>;
                    my $name  = $fn<name>;
                    my %args  = try from-json($fn<arguments>) // {};
                    my $tc-id = $tc<id> // 'unknown';

                    note "    ⚙️ {$name}";
                    my %result = exec-tool($name, $tc-id, %args);
                    @messages.push: {
                        :role<tool>,
                        :tool_call_id($tc-id),
                        :content(to-json(%result)),
                    };
                }

                note "  🔄 Reenviando para o model...";
                next;
            }

            if $finish eq 'stop' {
                @messages.push: $message;
                note "  ✅ {$worker-id} finalizou tarefa";
                last;
            }

            note "  ⚠️ finish_reason={$finish}";
            last;
        }

        # Return result to orchestrator
        my $response = to-json({
            :worker_id($worker-id),
            :role($role),
            :result($final-content),
        });
        $nats.publish: $reply-to, $response;
        note "  📤 {$worker-id} respondeu ao orchestrator";
    }
}
