#!/usr/bin/env raku
# 🌺 Camélia PoC #4 — Orchestrator (Worker Pool via JetStream)
#
# Decomposes tasks, publishes to JetStream stream, asks spawner
# to ensure workers, collects results, synthesizes final response.

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url     = %*ENV<NATS_URL>      // 'nats://127.0.0.1:4222';
my $max-workers  = %*ENV<MAX_WORKERS>    // 3;

# ── System prompts ──

my $decomp-system = q:to/END/;
You are a task orchestrator. Your job is to break down complex tasks into parallel subtasks.

Given a user request, decompose it into 2-3 INDEPENDENT subtasks that can be executed in parallel by worker agents.
Each worker can: run shell commands, read files, write files.

Output ONLY a JSON array of subtask objects:
[
  {
    "id": "task-1",
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
Be concise and direct.
END

# ── Connect NATS ──

note "🟡 Orchestrator connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;

my $task-sub = $nats.subscribe: 'orchestrator.task';
note "🟢 Orchestrator subscribed, entering react...";

# ── JetStream setup (infrastructure, runs once at startup) ──
setup-jetstream();

react {
    whenever $task-sub.supply -> $msg {
        next unless $msg.payload;
        my $reply-to = $msg.?reply-to;
        unless $reply-to {
            note "⚠️ orchestrator.task without reply-to, ignoring";
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

        note "📨 New task: {$prompt.substr(0, 100)}...";

        start {
            process-task($prompt, $reply-to);
        }
    }
}

# ═════════════════════════════════════════════
# JETSTREAM SETUP
# ═════════════════════════════════════════════

sub setup-jetstream() {
    note "📦 Setting up JetStream stream + consumer...";

    my $stream = Nats::Stream.new:
        :$nats,
        :name<WORKER_TASKS>,
        :subjects(['worker.task.>']),
        :retention<limits>,
        ;

    my $s-supply = $stream.create;
    my $s-msg = await $s-supply.Promise;
    note $s-msg ?? "  ✅ Stream: {$s-msg.payload}" !! "  ⚠️ Stream creation returned no message";

    note "📥 Creating ephemeral pull consumer...";
    my $consumer-subject = "\$JS.API.CONSUMER.CREATE.WORKER_TASKS.worker-pool";
    my $consumer-config = to-json({
        :stream_name<WORKER_TASKS>,
        :config{
            :ack_policy<explicit>,
            :deliver_policy<all>,
            :filter_subject('worker.task.>'),
            :max_ack_pending(1),
            :ack_wait(30_000_000_000),  # 30s in nanoseconds
            :replay_policy<instant>,
        },
    });
    my $c-resp = $nats.request($consumer-subject, $consumer-config);
    my $c-msg = await $c-resp.Promise;
    note $c-msg ?? "  ✅ Consumer created" !! "  ⚠️ Consumer creation returned no message";

    note "✅ JetStream ready.";
}

# ═════════════════════════════════════════════
# PROCESSING LOGIC
# ═════════════════════════════════════════════

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

sub spawn-workers(Int $count --> Hash) {
    my $inbox = "_INBOX.spawn." ~ (('a'..'z').pick xx 8).join;
    my $sub   = $nats.subscribe: $inbox;
    my $reply = start await $sub.supply.head.Promise;

    $nats.publish: 'spawner.control', to-json({
        :action<ensure>,
        :$count,
    }), :reply-to($inbox);

    my $msg = await $reply;
    $nats.unsubscribe: $sub;

    return { :error("No response from spawner") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("Spawner JSON parse fail") };
}

sub process-task(Str $prompt, Str $reply-to) {
    # ═══════ STEP 1: Decompose ═══════
    note "📋 Decomposing...";
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

    # ═══════ STEP 2: Publish tasks to JetStream + subscribe to results ═══════
    note "📤 Publishing {+@subtasks} tasks to worker.task.* stream...";

    my @result-promises;
    for @subtasks -> $st {
        my $task-id   = $st<id> // ('task-' ~ (^1000).pick);
        my $result-inbox = "_INBOX.result.{$task-id}." ~ (('a'..'z').pick xx 8).join;

        # Subscribe BEFORE publishing (avoids race)
        my $result-sub = $nats.subscribe: $result-inbox;
        my $result-promise = start {
            my $msg = await $result-sub.supply.head.Promise;
            $nats.unsubscribe: $result-sub;
            return { :error("Worker {$task-id}: no response") } unless $msg && $msg.payload;
            try from-json($msg.payload) // { :error("Bad result JSON") };
        };
        @result-promises.push: $result-promise;

        # Publish to JetStream subject
        $nats.publish: "worker.task.{$task-id}", to-json({
            :id($task-id),
            :role($st<role>),
            :task($st<task>),
            :reply-to($result-inbox),
        });

        note "  📤 {$task-id} → worker.task.{$task-id}";
    }

    # ═══════ STEP 3: Ensure workers ═══════
    my $needed = min(+@subtasks, $max-workers.Int);
    note "🚀 Asking spawner for {$needed} worker(s)...";
    my %spawn-resp = spawn-workers($needed);
    if %spawn-resp<ok> {
        note "  ✅ Spawner: {(%spawn-resp<workers> // []).join(', ')}";
    } else {
        note "  ⚠️ Spawner warning: {%spawn-resp<message> // 'unknown'}";
    }

    # ═══════ STEP 4: Collect results ═══════
    note "⏳ Waiting for {+@result-promises} worker result(s)...";
    my @results = await Promise.allof(@result-promises).then({ @result-promises.map(*.result) }).result;

    for @results -> $r {
        if $r<error> {
            note "  ❌ {$r<worker-id> // '?'}: {$r<error>}";
        } else {
            note "  ✅ {$r<worker-id> // '?'}: {$r<task-id> // '?'} done";
        }
    }

    # ═══════ STEP 5: Synthesize ═══════
    note "🧠 Synthesizing...";
    my $results-block = '';
    for @results.kv -> $i, $r {
        my $label = $r<worker-id> // "worker-{$i}";
        $results-block ~= "=== {$label} ({$r<role> // '?'}) ===\n";
        $results-block ~= to-json($r) ~ "\n\n";
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
    $nats.publish: $reply-to, to-json({ :result($final), :subtask_count(+@subtasks), :mode<jetstream-pool> });
    note "✅ Response sent to caller.";
}
