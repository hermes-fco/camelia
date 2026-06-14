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
my $model-subject = %*ENV<MODEL_SUBJECT> // 'model.deepseek.completion';

# ── System prompts ──

my $decomp-system = q:to/END/;
You are a task orchestrator. Break down complex tasks into parallel subtasks.

Available worker types:
- worker.shell — executes a SINGLE bash one-liner. The 'task' field MUST be a valid bash command (not a description). Chain with && or ; for multiple steps.

Given a user request — and optionally conversation history — decompose it into 2-3 INDEPENDENT subtasks that can be executed in parallel by worker agents.

Output ONLY a JSON array of subtask objects:
[
  {
    "id": "task-1",
    "role": "brief role description",
    "worker_type": "shell",
    "task": "echo hello && date"
  }
]

Rules:
- The 'task' field MUST be a runnable bash command — never a natural language instruction
- Use && to chain commands, ; for independent ones. Keep it under 500 chars
- Subtasks MUST be independent (no dependencies between them)
- Every subtask needs a "worker_type" field: "shell" for commands, "factory" for creating new workers
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
        my $chat-id    = %req<chat_id>    // '';

        unless $prompt {
            $nats.publish: $reply-to, to-json({ :error("Missing 'prompt' field") });
            next;
        }

        note "📨 New task: {$prompt.substr(0, 100)}..." ~
            ($session-id ?? " (session: $session-id)" !! "");

        # start {} isolates the await-heavy process-task from the react
        # event loop — other whenever blocks (health) remain responsive
        start {
            process-task($prompt, $reply-to, $session-id, $chat-id);
        }
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
    # If session_id provided, try to load it — MUST exist
    if $sid {
        my %resp = session-call('get', { :session_id($sid) });
        if %resp<ok> && %resp<session> {
            note "📂 Loaded session {$sid} (seq={%resp<session><seq>}, {+%resp<session><history>} history entries)";
            return %resp<session>;
        }
        # Session not found — retry once (JetStream propagation delay)
        note "⚠️ Session {$sid} not found on first attempt, retrying...";
        sleep 0.5;
        %resp = session-call('get', { :session_id($sid) });
        if %resp<ok> && %resp<session> {
            note "📂 Loaded session {$sid} on retry";
            return %resp<session>;
        }
        # Still not found — this is an error, don't silently create new
        note "❌ Session {$sid} not found after retry";
        return { :error("Session not found: {$sid}") };
    }

    # No session_id — create new session
    my %resp = session-call('create', {});
    if %resp<ok> && %resp<session> {
        note "🆕 New session: {%resp<session_id>}";
        return %resp<session>;
    }

    # Session-store unreachable
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

    await Promise.anyof: $p, Promise.in(120);

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
    request-reply($model-subject, to-json(%body));
}

sub spawn-workers(Int $count --> Hash) {
    request-reply('spawner.control', to-json({ :action<ensure>, :$count }));
}

# ═════════════════════════════════════════════
# PROCESSING LOGIC
# ═════════════════════════════════════════════

sub process-task(Str $prompt, Str $reply-to, Str $session-id?, Str $chat-id?) {
    # ═══════ SESSION: load from session-store ═══════
    my %session = session-load($session-id);
    if %session<error> {
        $nats.publish: $reply-to, to-json({ :error("Session-store unavailable: {%session<error>}"), :chat_id($chat-id) });
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
        $nats.publish: $reply-to, to-json({ :error($err), :chat_id($chat-id) });
        stream($sid, 'error', :message($err));
        return;
    }

    my $raw = %d-resp<choices>[0]<message><content> // '';
    $raw ~~ s/^ .*? '['/[/;
    $raw ~~ s/']' .*? $/]/;

    my @subtasks = try from-json($raw);
    if $! || @subtasks.elems == 0 {
        my $err = "Failed to parse subtasks: $!";
        $nats.publish: $reply-to, to-json({ :error($err), :chat_id($chat-id) });
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

    # ═══════ STEP 2: Publish tasks to typed workers (direct NATS request-reply) ═══════
    note "📤 Publishing {+@subtasks} tasks to typed workers...";

    my @result-promises;
    for @subtasks -> $st {
        my $task-id   = $st<id> // ('task-' ~ (^1000).pick);
        my $wtype     = $st<worker_type> // 'shell';
        my $result-inbox = "_INBOX.result.{$task-id}." ~ (('a'..'z').pick xx 8).join;

        my $result-sub = $nats.subscribe: $result-inbox, :1max-messages;
        my $result-promise = $result-sub.supply.head.Promise.then: -> $p {
            my $msg = $p.result;
            return { :error("Worker {$task-id}: no response") } unless $msg && $msg.payload;
            try from-json($msg.payload) // { :error("Bad result JSON") };
        };
        @result-promises.push: $result-promise;

        # Route to typed worker: worker.shell.task.<id>, worker.factory.request, etc.
        my $subject = $wtype eq 'factory'
            ?? 'worker.factory.request'
            !! "worker.{$wtype}.task.{$task-id}";

        my %payload = :id($task-id), :role($st<role>), :task($st<task>),
                      :session-id($sid);

        # Factory requests need different payload format
        if $wtype eq 'factory' {
            %payload = :prompt($st<task>), :spec({ :name($task-id), :description($st<role>) });
        }

        $nats.publish: $subject, to-json(%payload), :reply-to($result-inbox);

        note "  📤 {$task-id} ({$wtype}) → {$subject}";
    }

    # ═══════ STEP 3: Spawn workers for non-typed tasks only (JetStream) ═══════
    my $generic-tasks = @subtasks.grep({ !($_<worker_type> // '') }).elems;
    if $generic-tasks > 0 {
        my $needed = min($generic-tasks, $max-workers.Int);
        note "🚀 Asking spawner for {$needed} generic worker(s)...";
        my %spawn-resp = spawn-workers($needed);
        if %spawn-resp<ok> {
            note "  ✅ Spawner: {(%spawn-resp<workers> // []).join(', ')}";
            stream($sid, 'workers-ready',
                :message("{%spawn-resp<workers>.elems} worker(s) started"),
            );
        } else {
            note "  ⚠️ Spawner warning: {%spawn-resp<message> // 'unknown'}";
        }
    } else {
        note "✅ All tasks routed to typed workers (no spawner needed)";
    }

    # ═══════ STEP 4: Collect results ═══════
    note "⏳ Waiting for {+@result-promises} worker result(s) (60s timeout)...";
    my $all = Promise.allof(@result-promises);
    await Promise.anyof: $all, Promise.in(60);
    my @results = $all.so
        ?? @result-promises.map(*.result)
        !! [{ :error("Worker result timeout after 60s") },];

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

    # ═══════ STEP 5: Handle results — retry with spawner if no workers ═══════
    if $done-count == 0 {
        note "⚠️ No workers completed — requesting spawner to ensure typed workers...";

        # Collect unique worker types that failed
        my %failed-types;
        for @subtasks -> $st {
            my $wtype = $st<worker_type> // 'shell';
            %failed-types{$wtype}++;
        }

        my $spawner-ok = False;
        for %failed-types.keys -> $wtype {
            next if $wtype eq 'factory';  # factory is a meta-worker, skip
            my %sr = request-reply('spawner.control', to-json({ :action<ensure_typed>, :type($wtype) }));
            if %sr<ok> {
                note "  ✅ Spawner ensured worker type '{$wtype}': {%sr<status> // 'started'}";
                $spawner-ok = True;
            } else {
                note "  ⚠️ Spawner for '{$wtype}': {%sr<error> // 'unknown error'}";
                if %sr<reason> eq 'no_image' {
                    note "  🏭 No image for '{$wtype}' — requesting worker-factory...";
                    my %fr = request-reply('worker.factory.request', to-json({
                        :prompt("Create worker type {$wtype} for executing tasks"),
                        :spec({ :name($wtype), :description("Worker for {$wtype} tasks") }),
                    }));
                    if %fr<status> eq 'created' {
                        note "  ✅ Factory created '{$wtype}' — retrying spawner...";
                        sleep 2;
                        my %sr2 = request-reply('spawner.control', to-json({ :action<ensure_typed>, :type($wtype) }));
                        if %sr2<ok> { $spawner-ok = True }
                    }
                }
            }
        }

        if $spawner-ok {
            note "🔄 Spawner started workers — retrying tasks (15s timeout)...";
            sleep 3;  # Wait for workers to connect to NATS

            # Re-publish tasks (reuse the same result inbox pattern)
            @result-promises = ();
            for @subtasks -> $st {
                my $task-id   = $st<id> // ('task-' ~ (^1000).pick);
                my $wtype     = $st<worker_type> // 'shell';
                next if $wtype eq 'factory';
                my $result-inbox = "_INBOX.retry.{$task-id}." ~ (('a'..'z').pick xx 8).join;
                my $result-sub = $nats.subscribe: $result-inbox, :1max-messages;
                my $result-promise = $result-sub.supply.head.Promise.then: -> $p {
                    my $msg = $p.result;
                    return { :error("Worker {$task-id}: no response on retry") } unless $msg && $msg.payload;
                    try from-json($msg.payload) // { :error("Bad result JSON") };
                };
                @result-promises.push: $result-promise;
                my $subject = "worker.{$wtype}.task.{$task-id}";
                my %payload = :id($task-id), :role($st<role>), :task($st<task>), :session-id($sid);
                $nats.publish: $subject, to-json(%payload), :reply-to($result-inbox);
                note "  🔄 Retry {$task-id} → {$subject}";
            }

            my $retry-all = Promise.allof(@result-promises);
            await Promise.anyof: $retry-all, Promise.in(15);
            @results = $retry-all.so
                ?? @result-promises.map(*.result)
                !! [{ :error("Worker retry timeout after 15s") },];
            $done-count = 0;
            for @results -> $r {
                $done-count++ unless $r<error>;
            }
            note $done-count > 0
                ?? "  ✅ Retry: {$done-count}/{+@results} workers responded"
                !! "  ❌ Retry also failed";
        }

        if $done-count == 0 {
            note "⚠️ No workers completed after retry — graceful degradation";
            my $err-msg = "Unable to process your request right now — no workers available. Please try again later.";
            session-append-batch($sid, $seq, [
                { :role<user>,      :content($prompt) },
                { :role<assistant>, :content($err-msg) },
            ]);
            $nats.publish: $reply-to, to-json({
                :result($err-msg),
                :session_id($sid),
                :subtask_count(+@subtasks),
                :chat_id($chat-id),
            });
            stream($sid, 'degraded', :message("Graceful degradation: 0/{+@subtasks} workers completed after retry"));
            note "✅ Graceful degradation response sent (session {$sid}).";
            $tasks-done++;
            return;
        }
    }

    # ═══════ STEP 6: Synthesize (with history) ═══════
    note "🧠 Synthesizing...";
    stream($sid, 'synthesizing', :message("Synthesizing final response..."));

    my $results-block = '';
    for @results.kv -> $i, $r {
        my $label = $r<worker-id> // "worker-{$i}";
        $results-block ~= "=== {$label} ({$r<role> // '?'}) ===\n";
        $results-block ~= to-json $r ~ "\n\n";
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
        $nats.publish: $reply-to, to-json({ :error($err), :chat_id($chat-id) });
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
        :chat_id($chat-id),
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
