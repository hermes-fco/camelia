#!/usr/bin/env raku
# 🌺 Camélia PoC #3 — Orchestrator (Multi-Agent Delegation)
#
# Serviço long-running. Toda comunicação via NATS.
# Subscribe: orchestrator.task  → recebe prompt
# Reply:     inbox reply-to     → devolve resultado final

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222';

# ── System prompts ──

my $decomp-system = q:to/END/;
You are a task orchestrator. Your job is to break down complex tasks into parallel subtasks.

Given a user request, decompose it into 2-3 INDEPENDENT subtasks that can be executed in parallel by worker agents.
Each worker can: run shell commands, read files, write files.

Output ONLY a JSON array of subtask objects:
[
  {
    "id": "worker-1",
    "role": "brief role description",
    "task": "detailed self-contained instruction for the worker"
  }
]

Rules:
- Subtasks MUST be independent (no dependencies between them)
- Each subtask must be self-contained with all needed context
- Output ONLY the JSON array, nothing else — no markdown fences, no explanations
END

my $synth-system = q:to/END/;
You are a synthesis agent. Given the original request and individual worker results, combine them into a single coherent response.
Be concise and direct. Respond in Brazilian Portuguese.
END

# ── Connect NATS ──

note "🟡 Orchestrator conectando NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 Orchestrator pronto, aguardando tarefas em orchestrator.task...";

# ── Helper: call model (request/response via NATS) ──
sub call-model(@messages, :$temperature = 1.0, :@tools = (), :$tool_choice) {
    my %body = :model('deepseek-v4-pro'), :@messages, :$temperature;
    %body<tools>      = @tools if @tools;
    %body<tool_choice> = $tool_choice if $tool_choice;

    my $inbox = "_INBOX.orch." ~ (('a'..'z').pick xx 10).join;
    my $sub   = $nats.subscribe: $inbox;
    my $reply = start await $sub.supply.head.Promise;

    $nats.publish: 'model.deepseek.completion', to-json(%body), :reply-to($inbox);

    my $msg = await $reply;
    $nats.unsubscribe: $sub;

    return { :error("No response from model") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("JSON parse fail: $!") };
}

# ── Helper: spawn worker, wait for reply ──
sub call-worker(Str $id, Str $role, Str $task --> Hash) {
    my $subject = "worker.{$id}.task";
    my $inbox   = "_INBOX.orch.{$id}." ~ (('a'..'z').pick xx 8).join;
    my $sub     = $nats.subscribe: $inbox;
    my $promise = start {
        my $msg = await $sub.supply.head.Promise;
        $nats.unsubscribe: $sub;
        if $msg && $msg.payload {
            try from-json($msg.payload) // { :error("JSON parse fail in worker reply") }
        } else {
            { :error("Worker {$id} no response") }
        }
    };

    $nats.publish: $subject, to-json({ :$task, :$role }), :reply-to($inbox);
    note "  📤 {$id} → {$subject}";
    return await $promise;
}

# ── Core: process one orchestrator.task request ──
sub process-task(Str $prompt, Str $reply-to) {
    note "📨 Nova tarefa: {$prompt.substr(0, 100)}...";

    # ═══════ STEP 1: Decompose ═══════
    note "📋 Decompondo...";
    my @decomp-msgs = (
        { :role<system>, :content($decomp-system) },
        { :role<user>,   :content("Decompose this task into parallel subtasks: $prompt") },
    );

    my %d-resp = call-model(@decomp-msgs, :temperature(0.1));
    if %d-resp<error> {
        $nats.publish: $reply-to, to-json({ :error("Decomposition failed: {%d-resp<error>}") });
        return;
    }

    my $raw = %d-resp<choices>[0]<message><content> // '';
    $raw ~~ s/^ .*? '['/[/;
    $raw ~~ s/']' .*? $/]/;

    my @subtasks = try from-json($raw);
    if $! || @subtasks.elems == 0 {
        $nats.publish: $reply-to, to-json({ :error("Failed to parse subtasks: $!") });
        return;
    }

    note "✅ {+@subtasks} subtasks:";
    for @subtasks -> $st {
        note "  • {$st<id>} ({$st<role>})";
    }

    # ═══════ STEP 2: Spawn workers in parallel ═══════
    note "🚀 Spawnando {+@subtasks} workers em paralelo...";
    my @promises;
    for @subtasks -> $st {
        @promises.push: start {
            my $result = call-worker($st<id>, $st<role>, $st<task>);
            %( :id($st<id>), :role($st<role>), :$result )
        };
    }

    my @results = await @promises;
    for @results -> $r {
        if $r<result><error> {
            note "  ❌ {$r<id>}: {$r<result><error>}";
        } else {
            note "  ✅ {$r<id>} concluído";
        }
    }

    # ═══════ STEP 3: Synthesize ═══════
    note "🧠 Sintetizando...";
    my $results-block = '';
    for @results -> $r {
        $results-block ~= "=== {$r<id>} ({$r<role>}) ===\n";
        $results-block ~= to-json($r<result>) ~ "\n\n";
    }

    my @synth-msgs = (
        { :role<system>, :content($synth-system) },
        { :role<user>,   :content("Original request: $prompt\n\nWorker results:\n$results-block\n\nSynthesize a final response.") },
    );

    my %s-resp = call-model(@synth-msgs);
    if %s-resp<error> {
        $nats.publish: $reply-to, to-json({ :error("Synthesis failed: {%s-resp<error>}") });
        return;
    }

    my $final = %s-resp<choices>[0]<message><content> // '';
    $nats.publish: $reply-to, to-json({ :result($final), :subtask_count(+@subtasks) });
    note "✅ Resposta enviada ao caller.";
}

# ── React loop: aguarda tarefas ──

my $task-sub = $nats.subscribe: 'orchestrator.task';

react {
    whenever $task-sub.supply -> $msg {
        next unless $msg.payload;
        my $reply-to = $msg.?reply-to;
        unless $reply-to {
            note "⚠️ orchestrator.task sem reply-to, ignorando";
            next;
        }

        my %req = try from-json($msg.payload);
        if $! {
            $nats.publish: $reply-to, to-json({ :error("Invalid JSON") });
            next;
        }

        my $prompt = %req<prompt> // '';
        unless $prompt {
            $nats.publish: $reply-to, to-json({ :error("Missing 'prompt' field") });
            next;
        }

        process-task($prompt, $reply-to);
    }
}
