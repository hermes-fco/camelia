#!/usr/bin/env raku
# 🌺 Camélia PoC #3 — Worker Agent (long-running)
#
# Subscribes to worker.<WORKER_ID>.task, processes each task
# through the model+tool loop, returns final result via inbox.
# Uses react/whenever (same as model-deepseek).
# Spawns start{} inside whenever to keep react loop free.

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url  = %*ENV<NATS_URL>  // 'nats://127.0.0.1:4222';
my $worker-id = %*ENV<WORKER_ID> // 'default';
my $subject   = "worker.{$worker-id}.task";

# ── Tools schema ──
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
note "🟢 Worker {$worker-id} conectado.";

my $task-sub = $nats.subscribe: $subject;
note "🟢 Subscribed {$subject}, entering react...";

react {
    whenever $task-sub.supply -> $msg {
        next unless $msg.payload;
        my $reply-to = $msg.?reply-to;
        unless $reply-to {
            note "⚠️ No reply-to, ignorando";
            next;
        }

        my %req = try from-json($msg.payload);
        if $! {
            $nats.publish: $reply-to, to-json({ :error("Invalid JSON") });
            next;
        }

        my $task = %req<task> // 'Execute a task';
        my $role = %req<role> // 'worker';
        note "📨 {$worker-id}: {$role} — {$task.substr(0, 100)}...";

        # Process in a start block so react loop stays free
        start {
            process-one-task($nats, $task, $role, $worker-id, $reply-to);
        }
    }
}

# ── Process a single task (runs in a start block) ──
sub process-one-task($nats, Str $task, Str $role, Str $worker-id, Str $reply-to) {
    my @messages = (
        { :role<system>, :content($system ~ "\n\nSeu papel nesta tarefa: {$role}") },
        { :role<user>,   :content($task) },
    );

    my $final-content = '';
    my $max-turns = 8;

    loop {
        last if $max-turns-- <= 0;

        my %resp = call-model($nats, @messages);
        if %resp<error> {
            note "  ❌ Model error: {%resp<error>}";
            $final-content = "ERROR: {%resp<error>}";
            last;
        }

        my $choice = %resp<choices>[0];
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
                my %result = exec-tool($nats, $name, $tc-id, %args);
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

    $nats.publish: $reply-to, to-json({
        :$worker-id,
        :$role,
        :result($final-content),
    });
    note "  📤 {$worker-id} respondeu ao orchestrator";
}

# ── Helper: call model ──
sub call-model($nats, @messages --> Hash) {
    my $inbox = "_INBOX.wkr." ~ (('a'..'z').pick xx 10).join;
    my $sub   = $nats.subscribe: $inbox;
    my $reply = start await $sub.supply.head.Promise;

    $nats.publish: 'model.deepseek.completion', to-json({
        :id((^2**32).pick.fmt('%08x')),
        :model('deepseek-v4-pro'),
        :@messages,
        :@tools,
        :tool_choice<auto>,
    }), :reply-to($inbox);

    my $msg = await $reply;
    $nats.unsubscribe: $sub;

    return { :error("No response from model") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("JSON parse fail: $!") };
}

# ── Helper: execute a tool call ──
sub exec-tool($nats, Str $name, Str $tc-id, %args --> Hash) {
    my $inbox = "_INBOX.tl." ~ (('a'..'z').pick xx 10).join;
    my $sub   = $nats.subscribe: $inbox;
    my $reply = start await $sub.supply.head.Promise;

    $nats.publish: "tools.exec.{$name}", to-json({
        :$name,
        :tool_call_id($tc-id),
        :arguments(%args),
    }), :reply-to($inbox);

    my $msg = await $reply;
    $nats.unsubscribe: $sub;

    return { :error("No response from tool executor") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("JSON parse fail in tool result") };
}
