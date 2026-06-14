#!/usr/bin/env raku
# 🌺 Camélia PoC #6 — Orchestrator (Persistence + Session Store)
#
# Decomposes tasks, publishes to JetStream stream, asks spawner
# to ensure workers, collects results, synthesizes final response.
# Sessions persisted via session-store (isolated container).

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url     = %*ENV<NATS_URL>      // 'nats://127.0.0.1:4222';
my $max-workers  = %*ENV<MAX_WORKERS>    // 3;
my $start-time   = now;                  # for uptime metric
my $tasks-done   = 0;                    # completed task counter

# ── System prompts ──

my $decomp-system = q:to/END/;
You are a task orchestrator. Your job is to break down complex tasks into parallel subtasks.

Given a user request — and optionally conversation history — decompose it into 2-3 INDEPENDENT subtasks that can be executed in parallel by worker agents.
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
- If conversation history is provided, use it to maintain continuity
- Output ONLY the JSON array, nothing else — no markdown fences, no explanations
END

my $synth-system = q:to/END/;
You are a synthesis agent. Given the original request and individual worker results, combine them into a single coherent response.
Be concise and direct. If there is previous conversation history, maintain continuity.
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

# Health check + metrics
my $health-sub = $nats.subscribe: 'health.check.orchestrator';

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

        my $prompt     = %req<prompt>     // '';
        my $session-id = %req<session_id> // '';

        unless $prompt {
            $nats.publish: $reply-to, to-json({ :error("Missing 'prompt' field") });
            next;
        }

        note "📨 New task: {$prompt.substr(0, 100)}..." ~
            ($session-id ?? " (session: $session-id)" !! "");

        process-task($prompt, $reply-to, $session-id);
    }

    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            my $uptime = (now - $start-time).Int;
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>,
                :service<orchestrator>,
                :$uptime,
                :tasks_completed($tasks-done),
            });
        }
    }
}

# ═════════════════════════════════════════════
# STREAMING HELPER
# ═════════════════════════════════════════════

sub stream(Str $session-id, Str $status, :$message, :$data, :$subtask_count, :$result) {
    my %payload = :$status;
    %payload<message>       = $message       if $message;
    %payload<data>          = $data          if $data;
    %payload<subtask_count> = $subtask_count if $subtask_count;
    %payload<result>        = $result        if $result;

    my $subject = "session.{$session-id}.stream";
    $nats.publish: $subject, to-json(%payload);
}

# ═════════════════════════════════════════════
# SESSION STORE HELPERS (remote calls via NATS)
# ═════════════════════════════════════════════

sub session-load(Str $sid? --> Hash) {
    # If session_id provided, try to load it
    if $sid {
        my %resp = session-call('get', { :session_id($sid) });
        if %resp<ok> && %resp<session> {
            note "📂 Loaded session {$sid} (seq={%resp<session><seq>}, {+%resp<session><history>} history entries)";
            return %resp<session>;
        }
        # Session not found or store unreachable — create new
        note "⚠️ Session {$sid} not found, creating new";
    }

    # Create new session
    my %resp = session-call('create', {});
    if %resp<ok> && %resp<session> {
        note "🆕 New session: {%resp<session_id>}";
        return %resp<session>;
    }

    # Session-store unreachable — fail explicitly (no silent fallback)
    return { :error("Session-store unreachable") };
}

sub session-append-batch(Str $sid, Int $expected-seq, @entries --> Hash) {
    my %resp = session-call('append', {
        :session_id($sid),
        :expected_seq($expected-seq),
        :@entries,
    });
    if %resp<error> {
        note "⚠️ Session append failed: {%resp<error>}";
        if %resp<conflict> {
            note "  🔄 Seq conflict: expected {$expected-seq}, current {%resp<current_seq>}";
        }
    }
    return %resp;
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
# REQUEST-REPLY — uses $nats.request (native nats.raku)
# ═════════════════════════════════════════════

sub request-reply(Str $subject, Str $payload --> Hash) {
    my $supply = $nats.request: $subject, $payload;
    my $p = $supply.head.Promise;

    await Promise.anyof: $p, Promise.in(30);

    my %result = do if $p.so {
        my $msg = $p.result;
        if $msg && $msg.payload {
            try from-json($msg.payload) // { :error("JSON parse fail: $!") };
        } else {
            { :error("Empty response") };
        }
    } else {
        { :error("No response from {$subject}") };
    };

    return %result;
}
sub session-call(Str $op, %payload --> Hash) {
    request-reply("session.store.{$op}", to-json(%payload));
}

sub call-model(@messages, :$temperature = 1.0, :@tools = (), :$tool_choice) {
    my %body = :model('deepseek-v4-pro'), :@messages, :$temperature;
    %body<tools>      = @tools if @tools;
    %body<tool_choice> = $tool_choice if $tool_choice;
    request-reply('model.deepseek.completion', to-json(%body));
}

sub spawn-workers(Int $count --> Hash) {
    request-reply('spawner.control', to-json({ :action<ensure>, :$count }));
}

# ═════════════════════════════════════════════
# PROCESSING LOGIC
# ═════════════════════════════════════════════

sub process-task(Str $prompt, Str $reply-to, Str $session-id?) {
    # ═══════ SESSION: load from session-store ═══════
    my %session = session-load($session-id);
    if %session<error> {
        $nats.publish: $reply-to, to-json({ :error("Session-store unavailable: {%session<error>}") });
        stream($session-id || 'unknown', 'error', :message("Session-store unavailable"));
        return;
    }
    my $sid     = %session<session_id>;
    my $seq     = %session<seq> // 0;
    my @history = %session<history>.List;
    my $task-n  = (%session<task_count> // 0) + 1;

    stream($sid, 'received', :message("Task received: {$prompt.substr(0, 80)}..."));

    # ═══════ STEP 1: Decompose (with history from session-store) ═══════
    note "📋 Decomposing (session {$sid}, task #{$task-n})...";
    my @decomp-msgs = (
        { :role<system>, :content($decomp-system) },
    );
    for @history -> $entry {
        @decomp-msgs.push: $entry;
    }
    @decomp-msgs.push: { :role<user>, :content("Decompose this task into parallel subtasks: $prompt") };

    my %d-resp = call-model(@decomp-msgs, :temperature(0.1));
    if %d-resp<error> {
        my $err = "Decomposition failed: {%d-resp<error>}";
        $nats.publish: $reply-to, to-json({ :error($err) });
        stream($sid, 'error', :message($err));
        return;
    }

    my $raw = %d-resp<choices>[0]<message><content> // '';
    $raw ~~ s/^ .*? '['/[/;
    $raw ~~ s/']' .*? $/]/;

    my @subtasks = try from-json($raw);
    if $! || @subtasks.elems == 0 {
        my $err = "Failed to parse subtasks: $!";
        $nats.publish: $reply-to, to-json({ :error($err) });
        stream($sid, 'error', :message($err));
        return;
    }

    note "✅ {+@subtasks} subtasks:";
    for @subtasks -> $st {
        note "  • {$st<id>} ({$st<role>})";
    }

    stream($sid, 'decomposed',
        :message("Decomposed into {+@subtasks} subtasks"),
        :subtask_count(+@subtasks),
    );

    # ═══════ STEP 2: Publish tasks to JetStream + subscribe to results ═══════
    note "📤 Publishing {+@subtasks} tasks to worker.task.* stream...";

    my @result-promises;
    for @subtasks -> $st {
        my $task-id   = $st<id> // ('task-' ~ (^1000).pick);
        my $result-inbox = "_INBOX.result.{$task-id}." ~ (('a'..'z').pick xx 8).join;

        my $result-sub = $nats.subscribe: $result-inbox, :1max-messages;
        my $result-promise = $result-sub.supply.head.Promise.then: -> $p {
            my $msg = $p.result;
            return { :error("Worker {$task-id}: no response") } unless $msg && $msg.payload;
            try from-json($msg.payload) // { :error("Bad result JSON") };
        };
        @result-promises.push: $result-promise;

        $nats.publish: "worker.task.{$task-id}", to-json({
            :id($task-id),
            :role($st<role>),
            :task($st<task>),
            :reply-to($result-inbox),
            :session-id($sid),
        });

        note "  📤 {$task-id} → worker.task.{$task-id}";
    }

    # ═══════ STEP 3: Ensure workers ═══════
    my $needed = min(+@subtasks, $max-workers.Int);
    note "🚀 Asking spawner for {$needed} worker(s)...";
    my %spawn-resp = spawn-workers($needed);
    if %spawn-resp<ok> {
        note "  ✅ Spawner: {(%spawn-resp<workers> // []).join(', ')}";
        stream($sid, 'workers-ready',
            :message("{%spawn-resp<workers>.elems} worker(s) started"),
        );
    } else {
        note "  ⚠️ Spawner warning: {%spawn-resp<message> // 'unknown'}";
    }

    # ═══════ STEP 4: Collect results ═══════
    note "⏳ Waiting for {+@result-promises} worker result(s) (15s timeout)...";
    my $all = Promise.allof(@result-promises);
    await Promise.anyof: $all, Promise.in(15);
    my @results = $all.so
        ?? @result-promises.map(*.result)
        !! [{ :error("Worker result timeout after 15s") }];

    my $done-count = 0;
    for @results -> $r {
        if $r<error> {
            note "  ❌ {$r<worker-id> // '?'}: {$r<error>}";
        } else {
            $done-count++;
            note "  ✅ {$r<worker-id> // '?'}: {$r<task-id> // '?'} done";
        }
    }

    stream($sid, 'results-collected',
        :message("{$done-count}/{+@results} worker results collected"),
    );

    # ═══════ STEP 5: Handle results ═══════
    if $done-count == 0 {
        note "⚠️ No workers completed — graceful degradation";
        my $err-msg = "Unable to process your request right now — no workers available. Please try again later.";
        session-append-batch($sid, $seq, [
            { :role<user>,      :content($prompt) },
            { :role<assistant>, :content($err-msg) },
        ]);
        $nats.publish: $reply-to, to-json({
            :error($err-msg),
            :session_id($sid),
            :subtask_count(+@subtasks),
        });
        stream($sid, 'degraded', :message("Graceful degradation: 0/{+@subtasks} workers completed"));
        note "✅ Graceful degradation response sent (session {$sid}).";
        $tasks-done++;
        return;
    }

    # ═══════ STEP 6: Synthesize (with history) ═══════
    note "🧠 Synthesizing...";
    stream($sid, 'synthesizing', :message("Synthesizing final response..."));

    my $results-block = '';
    for @results.kv -> $i, $r {
        my $label = $r<worker-id> // "worker-{$i}";
        $results-block ~= "=== {$label} ({$r<role> // '?'}) ===\n";
        $results-block ~= to-json($r) ~ "\n\n";
    }

    my @synth-msgs = (
        { :role<system>, :content($synth-system ~ "\n\nSession ID: {$sid}") },
    );
    for @history -> $entry {
        @synth-msgs.push: $entry;
    }
    @synth-msgs.push: {
        :role<user>,
        :content("Original request: $prompt\n\nWorker results:\n$results-block\n\nSynthesize a final response."),
    };

    my %s-resp = call-model(@synth-msgs);
    if %s-resp<error> {
        my $err = "Synthesis failed: {%s-resp<error>}";
        $nats.publish: $reply-to, to-json({ :error($err) });
        stream($sid, 'error', :message($err));
        return;
    }

    my $final = %s-resp<choices>[0]<message><content> // '';

    # ── Persist session history (atomic batch, CAS) ──
    session-append-batch($sid, $seq, [
        { :role<user>,      :content($prompt) },
        { :role<assistant>, :content($final) },
    ]);

    # ── Send final response ──
    $nats.publish: $reply-to, to-json({
        :result($final),
        :session_id($sid),
        :subtask_count(+@subtasks),
        :mode<jetstream-pool>,
    });

    stream($sid, 'done',
        :message("Response ready"),
        :result($final),
        :subtask_count(+@subtasks),
    );

    note "✅ Response sent to caller (session {$sid}).";
    $tasks-done++;
}
