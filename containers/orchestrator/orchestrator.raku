#!/usr/bin/env raku
# 🌺 Camélia PoC #3 — Orchestrator (Multi-Agent Delegation)
#
# 1. Recebe prompt do usuário (env PROMPT)
# 2. Chama o model pra decompor em subtasks paralelas
# 3. Spawna workers via NATS (worker.<id>.task)
# 4. Coleta resultados de todos os workers
# 5. Chama o model de novo pra sintetizar resposta final

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222';
my $prompt   = %*ENV<PROMPT>   // 'Analise o diretório /root/camelia: liste e explique containers/ e lib/ em paralelo';

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
note "🟢 Orchestrator conectado.";

# ═════════════════════════════════════════════
# STEP 1: Decompose the task
# ═════════════════════════════════════════════

note "📋 Decompondo: $prompt";

my @decomp-msgs = (
    { :role<system>, :content($decomp-system) },
    { :role<user>,   :content("Decompose this task into parallel subtasks: $prompt") },
);

my $decomp-json = to-json {
    :id('decomp-1'),
    :model('deepseek-v4-pro'),
    :messages(@decomp-msgs),
    :temperature(0.1),
};

my $d-inbox = "_INBOX.orch." ~ (('a'..'z').pick xx 10).join;
my $d-sub   = $nats.subscribe: $d-inbox;
my $d-reply = start await $d-sub.supply.head.Promise;

$nats.publish: 'model.deepseek.completion', $decomp-json, :reply-to($d-inbox);

my $d-msg = await $d-reply;
$nats.unsubscribe: $d-sub;

die "❌ Sem resposta do model (decomposição)" unless $d-msg && $d-msg.payload;

my %d-resp = try from-json($d-msg.payload);
die "❌ JSON inválido: $!" if $!;
die "❌ Model error: {%d-resp<error>}" if %d-resp<error>;

my $raw = %d-resp<choices>[0]<message><content> // '';
note "DEBUG raw decomp: $raw";

# Extract JSON array — strip markdown fences if present
$raw ~~ s/^ .*? '['/[/;
$raw ~~ s/']' .*? $/]/;

my @subtasks = try from-json($raw);
die "❌ Falha ao parsear subtasks: $!" if $! || @subtasks.elems == 0;

note "✅ {+@subtasks} subtasks:";
for @subtasks -> $st {
    note "  • {$st<id>} ({$st<role>}): {$st<task>.substr(0, 80)}...";
}

# ═════════════════════════════════════════════
# STEP 2: Spawn workers in parallel
# ═════════════════════════════════════════════

note "";
note "🚀 Spawnando {+@subtasks} workers em paralelo...";

my @promises;
my @meta;

for @subtasks -> $st {
    my $wid      = $st<id>;
    my $subject  = "worker.{$wid}.task";
    my $w-inbox  = "_INBOX.{$wid}." ~ (('a'..'z').pick xx 8).join;
    my $w-sub    = $nats.subscribe: $w-inbox;
    my $w-promise = start {
        my $w-msg = await $w-sub.supply.head.Promise;
        $nats.unsubscribe: $w-sub;
        if $w-msg && $w-msg.payload {
            try from-json($w-msg.payload) // { :error("JSON parse fail in worker reply") }
        } else {
            { :error("Worker {$wid} no response") }
        }
    };

    $nats.publish: $subject, to-json({
        :task($st<task>),
        :role($st<role>),
    }), :reply-to($w-inbox);

    @promises.push: $w-promise;
    @meta.push: { :id($wid), :role($st<role>), :promise($w-promise) };
    note "  📤 {$wid} → {$subject}";
}

# Collect all
note "⏳ Aguardando workers...";
my @results;
for @meta -> $m {
    my $result = await $m<promise>;
    @results.push: %( :id($m<id>), :role($m<role>), :result($result) );
    if $result<error> {
        note "  ❌ {$m<id>}: {$result<error>}";
    } else {
        note "  ✅ {$m<id>} concluído";
    }
}

# ═════════════════════════════════════════════
# STEP 3: Synthesize final response
# ═════════════════════════════════════════════

note "";
note "🧠 Sintetizando resposta final...";

my $results-block = '';
for @results -> $r {
    $results-block ~= "=== {$r<id>} ({$r<role>}) ===\n";
    $results-block ~= to-json($r<result>) ~ "\n\n";
}

my @synth-msgs = (
    { :role<system>, :content($synth-system) },
    { :role<user>,   :content("Original request: $prompt\n\nWorker results:\n$results-block\n\nSynthesize a final response.") },
);

my $synth-json = to-json {
    :id('synth-1'),
    :model('deepseek-v4-pro'),
    :messages(@synth-msgs),
};

my $s-inbox = "_INBOX.orch." ~ (('a'..'z').pick xx 10).join;
my $s-sub   = $nats.subscribe: $s-inbox;
my $s-reply = start await $s-sub.supply.head.Promise;

$nats.publish: 'model.deepseek.completion', $synth-json, :reply-to($s-inbox);

my $s-msg = await $s-reply;
$nats.unsubscribe: $s-sub;

if $s-msg && $s-msg.payload {
    my %s-resp = try from-json($s-msg.payload);
    if %s-resp<choices>[0]<message><content> -> $final {
        say $final;
        note "✅ Síntese concluída.";
    }
} else {
    note "❌ Falha na síntese.";
}

$nats.stop;
