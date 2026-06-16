#!/usr/bin/env raku
# 🌺 Camélia PoC #6 — Orchestrator (react + try guards)
#
# Decomposes tasks, publishes to JetStream stream, asks spawner
# to ensure workers, collects results, synthesizes final response.
# Sessions persisted via session-store (isolated container).

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url     = %*ENV<NATS_URL>      // 'nats://127.0.0.1:4222';
my $max-workers  = %*ENV<MAX_WORKERS>    // 3;
my $start-time   = now;
my $tasks-done   = 0;
my $model-subject = %*ENV<MODEL_SUBJECT> // 'model.deepseek.completion';

# ── Worker Registry ──
my %workers = (
    factory => {
        :name<factory>,
        :subject('worker.factory.request'),
        :description('Creates new worker types from specifications. Use to add capabilities'),
        :topics([]),
    },
);

sub build-decomp-prompt(--> Str) {
    my @lines = q:to/END/.lines;
You are a task orchestrator. Break down complex tasks into parallel subtasks.

Available worker types:
END

    for %workers.values.sort({ .<name> }) -> %w {
        my $name = %w<name>;
        my $desc = %w<description> // '';
        my @topics = %w<topics>.List;
        if @topics {
            @lines.push: "- **{$name}**: {$desc}. Topics: {@topics.join(', ')}";
        } else {
            @lines.push: "- **{$name}**: {$desc}";
        }
    }

    @lines.push: '';
    @lines.push: q:to/END/;
Rules:
- Decompose into PARALLEL subtasks: use different worker types for independent work.
- NEVER assign sequential dependencies between parallel tasks — they run simultaneously.
- WORKERS ARE ISOLATED: each runs in its own container with NO shared filesystem.
  Subtasks MUST NOT pass data through files (no "write to /tmp/x", "read from /tmp/x").
  All inter-worker data exchange goes through the task-store automatically.
- For any text-only question (no action/shell needed), return ZERO subtasks (conversational).
- Small tasks (<3 steps) are usually conversational — synthesize directly.
- NEVER use '??' as worker type — pick the closest matching real type.
- Only use worker types listed above. If none fit, use 'shell'.
- Output ONLY valid JSON array. No markdown, no explanation.

JSON format:
[{"id": "task-N", "role": "short name", "task": "detailed instruction", "worker_type": "shell|web-browser|timer|system|factory"}]
END
    @lines.join("\n")
}

sub build-synth-prompt(Str $sid) {
    q:to/END/
You are Camélia, a helpful AI assistant running on a multi-agent framework.
You have access to parallel workers, persistent sessions, and JetStream messaging.

Rules:
- Respond in the user's language (default: Portuguese/Brazil).
- Be concise and direct. No verbose introductions.
- If you used workers to get results, cite them briefly.
- Use markdown formatting for readability.
- NEVER say 'I'll let you know when it's done' — results are delivered instantly.
- NEVER suggest file-based communication between workers (no "save to file", "write to disk").
  Workers are isolated containers with no shared filesystem.
END
}

# ═════════════════════════════════════════════
# NATS HELPERS
# ═════════════════════════════════════════════

my $nats = Nats.new: :servers[$nats-url];
note "🟡 Orchestrator connecting NATS ($nats-url)...";
await $nats.start;
$nats.connect;
note "🟢 NATS connected.";

sub request-reply(Str $subject, Str $payload, :$timeout = 120 --> Hash) {
    my $inbox = gen-inbox();
    my $sub = $nats.subscribe($inbox, :1max-messages);
    $nats.publish($subject, $payload, :reply-to($inbox));
    my $p = $sub.supply.head.Promise;
    await Promise.anyof: $p, Promise.in($timeout);
    if $p.so {
        my $msg = $p.result;
        $sub.unsubscribe;
        return try from-json($msg.payload) // { :error("Invalid JSON response") };
    } else {
        $sub.unsubscribe;
        return { :error("Timeout after {$timeout}s on {$subject}") };
    }
}

# ═════════════════════════════════════════════
# JETSTREAM SETUP
# ═════════════════════════════════════════════

my constant @WORKER-TYPES = <shell web-browser timer system>;
my $task-chan = Channel.new;
my $task-consumer;

sub setup-jetstream() {
    note "📦 Setting up JetStream streams (per worker type)...";

    for @WORKER-TYPES -> $wtype {
        my $stream-name = "WORKER_" ~ $wtype.uc.subst('-', '_');
        my $subject = "worker.{$wtype}.task.>";

        my $ws = Nats::Stream.new:
            :$nats, :name($stream-name),
            :subjects([$subject]),
            :retention<workqueue>,
            :max-age(3_600_000_000_000),
            ;
        my $wss = $ws.create;
        my $wsm = await $wss.Promise;
        note $wsm ?? "  ✅ Stream {$stream-name} ({$subject})" !! "  ⚠️ {$stream-name}";

        note "  📥 Creating durable consumer worker-pool-{$wtype}...";
        my $consumer = Nats::Consumer.new:
            :$nats, :name("worker-pool-{$wtype}"), :stream($stream-name),
            :ack-policy<explicit>, :deliver-policy<all>,
            :filter-subject($subject),
            :max-ack-pending(10), :ack-wait(120),
            ;
        my $cs = $consumer.create-named;
        my $cm = await $cs.Promise;
        note ($cm && $cm.payload && !$cm.payload.starts-with("-ERR"))
            ?? "  ✅ Consumer worker-pool-{$wtype}"
            !! "  ⚠️ Consumer: {$cm.?payload // "no response"}";
    }

    note "✅ JetStream ready ({+@WORKER-TYPES} worker streams).";

    # ── Orchestrator own task stream + consumer ──
    my $ts = Nats::Stream.new:
        :$nats, :name<ORCHESTRATOR_TASKS>,
        :subjects(["orchestrator.task"]),
        :retention<limits>, :max-age(86_400_000_000_000),
        ;
    my $tss = $ts.create; my $tsm = await $tss.Promise;
    note $tsm ?? "  ✅ Stream ORCHESTRATOR_TASKS" !! "  ⚠️ ORCHESTRATOR_TASKS";

    $task-consumer = Nats::Consumer.new:
        :$nats, :name<orchestrator-main>, :stream<ORCHESTRATOR_TASKS>,
        :ack-policy<explicit>, :deliver-policy<new>,
        :filter-subject("orchestrator.task"),
        :max-ack-pending(10), :ack-wait(120), :replay-policy<instant>,
        ;
    my $tcs = $task-consumer.create-named;
    my $tcm = await $tcs.Promise;
    note ($tcm && $tcm.payload && !$tcm.payload.starts-with("-ERR"))
        ?? "  ✅ Consumer orchestrator-main"
        !! "  ⚠️ Consumer: {$tcm.?payload // "no response"}";

    # ── Worker Registry KV ──
    note "📋 Creating Worker Registry KV...";
    my $kv = Nats::Stream.new:
        :$nats, :name<KV_WORKER_REGISTRY>,
        :subjects(["\$KV.WORKER_REGISTRY.>"]),
        :retention<limits>,
        :max-msgs-per-subject(1),
        :discard<new>,
        ;
    my $kvs = $kv.create;
    my $kvm = await $kvs.Promise;
    note $kvm ?? "  ✅ KV KV_WORKER_REGISTRY" !! "  ⚠️ KV_WORKER_REGISTRY";

    note "  🌱 Seeding default workers...";
    my %defaults = (
        shell => { :name<shell>, :subject("worker.shell.task.>"),
                   :description("Executes a SINGLE bash one-liner. Chain with && or ;"), :topics([]) },
        "web-browser" => { :name<web-browser>, :subject("worker.web-browser.task.>"),
                   :description("Fetches URLs, renders JavaScript, extracts readable text"), :topics([]) },
        system => { :name<system>, :subject("worker.system.task.>"),
                   :description("Queries Camelia system: containers, health, sessions, reconfig"),
                   :topics(["containers_list","container_detail","system_health","session_get","session_list","reconfigure"]) },
        timer => { :name<timer>, :subject("worker.timer.task.>"),
                   :description("Sets timers and notifies user when they fire."), :topics([]) },
        factory => { :name<factory>, :subject("worker.factory.request"),
                   :description("Creates new worker types from specifications"), :topics([]) },
    );
    for %defaults.kv -> $name, %spec {
        $nats.publish: "\$KV.WORKER_REGISTRY.{$name}", to-json(%spec);
        %workers{$name} = %spec;
        note "    📌 {$name}";
    }
    note "  ✅ Registry seeded (%workers.elems() workers).";

    # Start pull loop to feed task supply
    start {
        loop {
            try {
                $task-consumer.msgs(:batch(5), :expires(30)).tap: -> $msg {
                    try $task-chan.send($msg) if $msg.?payload;
                };
            }
            sleep 0.5;
        }
    }
    note "  ✅ Consumer pull loop active";
}

# ═════════════════════════════════════════════
# TASK-STORE / SESSION-STORE HELPERS
# ═════════════════════════════════════════════

sub task-store-create(Str $prompt, :$session-id, :$chat-id, :$priority, :$worker-type --> Str) {
    request-reply('task.store.create', to-json({
        :description($prompt), :$session-id, :$chat-id, :$priority,
        :worker_type($worker-type // 'orchestrator'),
    }), :timeout(5))<id> // ''
}

sub task-store-create-subtask(Str $task-id, Str $description, Str $worker-type,
                               Str :$session-id, Str :$chat-id --> Str) {
    request-reply('task.store.create', to-json({
        :id($task-id), :$description,
        :worker_type($worker-type),
        :$session-id, :$chat-id, :priority(5), :created_by('orchestrator'),
    }), :timeout(5))<id> // ''
}

sub task-store-get(Str $task-id --> Hash) {
    request-reply('task.store.get', to-json({ :id($task-id) }), :timeout(5))
}

sub task-store-update(Str $task-id, Str $status, :$result, :$error --> Nil) {
    return unless $task-id;
    my %body = :$status;
    %body<result> = $result if $result;
    %body<error_msg> = $error  if $error;
    try request-reply('task.store.update', to-json({ :id($task-id), |%body }), :timeout(5));
}

sub session-load(Str $session-id --> Hash) {
    return { :session_id($session-id), :seq(0), :history([]) } unless $session-id;
    my %resp = request-reply('session.store.get', to-json({ :$session-id }), :timeout(5));
    if %resp<error> {
        return { :session_id($session-id), :seq(0), :history([]) };
    }
    my %session = %resp<session> // {};
    %session<session_id> //= $session-id;
    %session<seq> //= 0;
    %session<history> //= [];
    %session
}

sub session-append-batch(Str $session-id, Int $seq, @messages --> Nil) {
    return unless $session-id;
    try request-reply('session.store.append', to-json({
        :$session-id, :expected_seq($seq), :entries(@messages),
    }), :timeout(5));
}

# ═════════════════════════════════════════════
# MODEL HELPER
# ═════════════════════════════════════════════

sub call-model(@messages, :$temperature = 1.0, :@tools = (), :$tool_choice) {
    my %body = :model('deepseek-v4-pro'), :@messages, :$temperature;
    %body<tools>      = @tools if @tools;
    %body<tool_choice> = $tool_choice if $tool_choice;
    request-reply($model-subject, to-json(%body));
}

# ═════════════════════════════════════════════
# NOTIFICATION HELPER (entry-telegram listens here)
# ═════════════════════════════════════════════

sub notify-user(Str $reply-to, Str $chat-id, Str $message, Str :$edit_key) {
    return unless $reply-to && $chat-id && $message;
    my %payload = :text($message), :chat_id($chat-id), :parse_mode<markdown>;
    %payload<edit_key> = $edit_key if $edit_key;
    $nats.publish: $reply-to, to-json(%payload);
}

# ═════════════════════════════════════════════
# STREAMING HELPER
# ═════════════════════════════════════════════

sub stream(Str $session-id, Str $status, :$message, :$data, :$subtask_count, :$result) {
    $nats.publish: 'stream.update', to-json({
        :$session-id, :$status,
        :message($message // ''),
        :data($data // ''),
        :subtask_count($subtask_count // 0),
        :result($result // ''),
        :ts(now.Int),
    });
}

# ═════════════════════════════════════════════
# SYNTHESIS
# ═════════════════════════════════════════════

sub synthesize-and-respond(Str $prompt, Str $reply-to, Str $sid, Int $seq,
                            Str $chat-id, @history, @subtasks --> Nil) {
    note "🧠 Direct synthesis (conversational)...";
    stream($sid, 'synthesizing', :message("Generating direct response..."));

    my @synth-msgs = (
        { :role<system>, :content(build-synth-prompt($sid)) },
    );
    for @history -> $entry {
        @synth-msgs.push: $entry;
    }
    @synth-msgs.push: {
        :role<user>,
        :content("Respond directly to this message: {$prompt}"),
    };

    my %s-resp = call-model(@synth-msgs);
    if %s-resp<error> {
        my $err = "Synthesis failed: {%s-resp<error>}";
        $nats.publish: $reply-to, to-json({ :error($err), :chat_id($chat-id) });
        stream($sid, 'error', :message($err));
        return;
    }

    my $final = %s-resp<choices>[0]<message><content> // '';

    session-append-batch($sid, $seq, [
        { :role<user>,      :content($prompt) },
        { :role<assistant>, :content($final) },
    ]);

    $nats.publish: $reply-to, to-json({
        :result($final),
        :session_id($sid),
        :subtask_count(0),
        :chat_id($chat-id),
        :mode<direct>,
    });

    stream($sid, 'done', :message("Direct response ready"), :result($final), :subtask_count(0));
    note "✅ Direct response sent (session {$sid}).";
}

# ═════════════════════════════════════════════
# PROCESSING LOGIC
# ═════════════════════════════════════════════

sub process-task(Str $prompt, Str $reply-to, Str $session-id?, Str $chat-id?, Str $task-store-id?) {
    my %session = session-load($session-id);
    if %session<error> || !%session<session_id> {
        task-store-update($task-store-id, 'failed', :error("Session-store unavailable")) if $task-store-id;
        $nats.publish: $reply-to, to-json({ :error("Session-store unavailable"), :chat_id($chat-id) });
        return;
    }
    my $sid     = %session<session_id>;
    my $seq     = %session<seq> // 0;
    my @history = %session<history>.List;
    my $task-n  = (%session<task_count> // 0) + 1;

    stream($sid, 'received', :message("Task received: {$prompt.substr(0, 80)}..."));

    # Notify user
    notify-user($reply-to, $chat-id, "📋 *Analisando:* _{$prompt.substr(0, 100)}..._");

    # STEP 1: Decompose
    note "📋 Decomposing (session {$sid}, task #{$task-n})...";
    my $decomp-prompt = build-decomp-prompt();
    my @decomp-msgs = ({ :role<system>, :content($decomp-prompt) },);
    for @history -> $entry { @decomp-msgs.push: $entry }
    @decomp-msgs.push: { :role<user>, :content("Decompose this task into parallel subtasks: $prompt") };

    my %d-resp = call-model(@decomp-msgs, :temperature(0.1));
    if %d-resp<error> {
        my $err = "Decomposition failed: {%d-resp<error>}";
        $nats.publish: $reply-to, to-json({ :error($err), :chat_id($chat-id) });
        stream($sid, 'error', :message($err));
        return;
    }

    my $content = %d-resp<choices>[0]<message><content> // '';
    $content ~~ s/^ \`\`\` json \s* //;
    $content ~~ s/ \`\`\` $ //;
    my @subtasks = try from-json($content).List;
    if $! {
        my $err = "Failed to parse decomposition JSON: $!";
        note "  💥 {$err}";
        $nats.publish: $reply-to, to-json({ :error($err), :chat_id($chat-id) });
        stream($sid, 'error', :message($err));
        return;
    }

    # Conversational path
    if @subtasks.elems == 0 {
        note "💬 Conversational — no subtasks, direct synthesis";
        notify-user($reply-to, $chat-id, "💬 *Pensando...*");
        stream($sid, 'decomposed', :message("Conversational"), :subtask_count(0));
        synthesize-and-respond($prompt, $reply-to, $sid, $seq, $chat-id, @history, []);
        return;
    }

    note "✅ {+@subtasks} subtasks:";
    for @subtasks -> $st { note "  • {$st<id>} ({$st<role>})" }

    stream($sid, 'decomposed', :message("Decomposed into {+@subtasks} subtasks"), :subtask_count(+@subtasks));

    # Notify with worker info
    my @worker-names = @subtasks.map({ $_<worker_type> // '?' }).unique;
    my $has-timer = @subtasks.first({ $_<worker_type> eq "timer" });
    my $progress-key = "progress-{$sid}";
    my $edit-key = $has-timer ?? "timer-{" ~ $has-timer<id> ~ "}" !! Str;
    my $workers-str = @worker-names.join(', ');
    my $workers-label = @worker-names.elems == 1 ?? 'worker' !! 'workers';
    notify-user($reply-to, $chat-id,
        "⏳ *Processando...*\n" ~
        "{+@subtasks} subtarefas · {@worker-names.elems} {$workers-label}\n" ~
        "`{$workers-str}`",
        :edit_key($edit-key));

    # STEP 2: Create subtasks in task-store (workers claim via task.store.next)
    # Also notify spawner via worker.<type>.task.> ping so it knows to spawn workers
    note "📤 Creating {+@subtasks} subtasks in task-store...";

    my @subtask-entries;  # { id, worker_type, done, result }
    for @subtasks -> $st {
        my $task-id   = $st<id> // ('task-' ~ (^1000).pick);
        my $wtype     = $st<worker_type> // 'shell';

        my $created = task-store-create-subtask($task-id, $st<task>, $wtype,
            :session-id($sid), :chat-id($chat-id));
        if $created {
            @subtask-entries.push: { :id($task-id), :worker_type($wtype), :done(False), :result(Nil) };
            note "  📝 {$task-id} ({$wtype}) → task-store";
            # Ping spawner: notify worker.<type>.task.<id> so spawner creates worker if needed
            $nats.publish: "worker.{$wtype}.task.{$task-id}", to-json({ :type<ping>, :$task-id });
        } else {
            note "  ⚠️ Failed to create subtask {$task-id} — will skip";
        }
    }

    unless @subtask-entries {
        my $err = "Failed to create any subtasks in task-store";
        $nats.publish: $reply-to, to-json({ :error($err), :chat_id($chat-id) });
        stream($sid, 'error', :message($err));
        task-store-update($task-store-id, 'failed', :error($err)) if $task-store-id;
        return;
    }

    # STEP 3: Poll task-store until all subtasks complete
    note "⏳ Waiting for {+@subtask-entries} subtask(s) (300s timeout)...";
    my $total-timeout = 300;
    my $poll-interval = 5;
    my $notify-every  = 30;
    my $last-notify   = 0;
    my $elapsed = 0;

    while $elapsed < $total-timeout {
        my $all-done = True;
        my $done-count = 0;
        for @subtask-entries -> $entry {
            next if $entry<done>;

            my %resp = task-store-get($entry<id>);
            if %resp<ok> && %resp<task> {
                my $status = %resp<task><status> // '';
                if $status eq 'completed' {
                    $entry<done> = True;
                    $entry<result> = %resp<task><result> // '';
                    $done-count++;
                    note "  ✅ {$entry<id>} completed";
                } elsif $status eq 'failed'|'cancelled' {
                    $entry<done> = True;
                    $entry<result> = { :error(%resp<task><error_msg> // $status) };
                    $done-count++;
                    note "  ❌ {$entry<id>} {%resp<task><error_msg> // $status}";
                }
            }
            unless $entry<done> { $all-done = False }
        }

        last if $all-done;
        await Promise.in($poll-interval);
        $elapsed += $poll-interval;

        if !$all-done && $elapsed < $total-timeout && $elapsed - $last-notify >= $notify-every {
            my $pending = +@subtask-entries - $done-count;
            my @pending-types = @subtask-entries.grep({ !.<done> }).map({ .<worker_type> }).unique.sort;
            my $pw-str = @pending-types.join(', ');
            my $msg = $done-count > 0
                ?? "⏳ *{$done-count}/{+@subtask-entries} concluído*\\nDecorrido: {$elapsed}s\\nPendente: `{$pw-str}`"
                !! "⏳ *Aguardando workers...*\\nDecorrido: {$elapsed}s\\nPendente: `{$pw-str}`";
            notify-user($reply-to, $chat-id, $msg, :edit_key($progress-key));
            $last-notify = $elapsed;
        }
    }

    if $elapsed >= $total-timeout {
        note "⚠️ Timeout after {$elapsed}s — collecting partial results";
        notify-user($reply-to, $chat-id,
            "⚠️ *Timeout após {$elapsed}s*\\nColetando resultados parciais...",
            :edit_key($progress-key));
    }

    # STEP 4: Collect results from completed entries
    my @results;
    for @subtask-entries -> $entry {
        if $entry<done> && $entry<result> {
            if $entry<result> ~~ Hash {
                @results.push: $entry<result>;
            } else {
                @results.push: { :output($entry<result>.Str), :task_id($entry<id>), :worker_type($entry<worker_type>) };
            }
        } else {
            @results.push: { :error("Task {$entry<id>} timed out after {$elapsed}s"), :task_id($entry<id>), :worker_type($entry<worker_type>) };
        }
    }

    my $successes = @results.grep({ !.<error> }).elems;
    note "📊 Collected {$successes}/{+@results} results successfully.";

    if $successes == 0 {
        my $err = "All {+@results} workers failed";
        $nats.publish: $reply-to, to-json({ :error($err), :chat_id($chat-id) });
        stream($sid, 'error', :message($err));
        return;
    }

    # STEP 5: Synthesize
    if +@subtasks == 1 && $successes == 1 && !@results[0]<error> {
        my $result = @results[0];
        my $final = $result<result> // $result<output> // to-json($result);

        session-append-batch($sid, $seq, [
            { :role<user>, :content($prompt) },
            { :role<assistant>, :content($final) },
        ]);

        $nats.publish: $reply-to, to-json({
            :result($final), :session_id($sid), :subtask_count(1),
            :chat_id($chat-id), :mode<single-worker>,
        });

        task-store-update($task-store-id, 'completed', :result($final.substr(0, 200))) if $task-store-id;
        stream($sid, 'done', :message("Single worker response ready"), :result($final), :subtask_count(1));
        note "✅ Single-worker response sent (session {$sid}).";
        $tasks-done++;
        return;
    }

    note "🧠 Synthesizing...";
    stream($sid, 'synthesizing', :message("Synthesizing final response..."));

    my $results-block = '';
    for @results.kv -> $i, $r {
        my $label = $r<task_id> // $r<worker_type> // "result-{$i}";
        $results-block ~= "=== {$label} ===\n";
        $results-block ~= to-json $r ~ "\n\n";
    }

    my @synth-msgs = ({ :role<system>, :content(build-synth-prompt($sid)) },);
    for @history -> $entry { @synth-msgs.push: $entry }
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

    session-append-batch($sid, $seq, [
        { :role<user>, :content($prompt) },
        { :role<assistant>, :content($final) },
    ]);

    $nats.publish: $reply-to, to-json({
        :result($final), :session_id($sid),
        :subtask_count(+@subtasks), :chat_id($chat-id),
        :mode<jetstream-pool>,
    });

    task-store-update($task-store-id, 'completed', :result($final.substr(0, 200))) if $task-store-id;

    stream($sid, 'done', :message("Response ready"), :result($final), :subtask_count(+@subtasks));
    note "✅ Response sent to caller (session {$sid}).";
    $tasks-done++;
}

# ═════════════════════════════════════════════
# MAIN: REACT LOOP
# ═════════════════════════════════════════════

my @inbox-chars = |("a" .. "z"), |("A" .. "Z"), |("0" .. "9"), "_";
sub gen-inbox(--> Str) { "_INBOX." ~ (@inbox-chars.pick xx 32).join }

my $health-sub    = $nats.subscribe: 'health.check.orchestrator';
my $registry-sub  = $nats.subscribe: '$KV.WORKER_REGISTRY.>';

# JetStream setup first, then enter react
setup-jetstream();

note "🟢 Orchestrator entering react...";

react {
    # ── Task processing (from JetStream consumer via Channel) ──
    whenever $task-chan -> $msg {
        next unless $msg.payload;

        my $parsed = try from-json($msg.payload);
        unless $parsed.defined {
            note "⚠️ Invalid JSON in task message";
            $task-consumer.ack($msg) if $task-consumer;
            next;
        }
        my %req = $parsed;

        my $prompt     = %req<prompt>     // '';
        my $session-id = %req<session_id> // '';
        my $chat-id    = %req<chat_id>    // '';
        my $reply-to   = %req<reply_to>   // $msg.?reply-to // '';

        unless $prompt { note "⚠️ Task without prompt"; next }
        unless $reply-to { note "⚠️ Task without reply_to"; next }

        note "📨 New task: {$prompt.substr(0, 100)}..." ~
            ($session-id ?? " (session: $session-id)" !! "");

        $task-consumer.ack($msg) if $task-consumer;

        my $task-id = task-store-create($prompt, :$session-id, :$chat-id,
            :priority(5), :worker-type('orchestrator'));
        note $task-id ?? "  📝 Task {$task-id} registered" !! "  ⚠️ Task-store unavailable";

        start {
            CATCH {
                default {
                    note "💥 process-task crashed: {.message}";
                    task-store-update($task-id, 'failed', :error(.message)) if $task-id;
                    try $nats.publish: $reply-to, to-json({ :error("Internal error: {.message}"), :chat_id($chat-id) });
                }
            }
            process-task($prompt, $reply-to, $session-id, $chat-id, $task-id);
        }
    }

    # ── Health check ──
    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            my $uptime = (now - $start-time).Int;
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service<orchestrator>, :$uptime,
                :tasks_completed($tasks-done), :registered_workers(%workers.elems),
            });
        }
    }

    # ── Worker registry ──
    whenever $registry-sub.supply -> $msg {
        my $parsed = try from-json($msg.payload);
        next unless $parsed.defined;
        my %w = $parsed;
        next unless %w<name>;
        my $name = %w<name>;
        %workers{$name} = %w;
        note "📋 Registry: +{$name} (%workers.elems() total: {%workers.keys.sort.join(', ')})";
    }
}
