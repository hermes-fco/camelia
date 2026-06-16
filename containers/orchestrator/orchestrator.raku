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

# ── Worker Registry (dynamic — updated as workers register) ──
# Worker metadata stored in KV_WORKER_REGISTRY (JetStream KV)
# Orchestrator uses this to build decomposition prompt dynamically.
# factory is always available (hardcoded fallback).
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
        my @topics = (%w<topics> // []).List;

        @lines.push: "- {$name} — {$desc}";
        if @topics {
            @lines.push: "  topics: {@topics.join(', ')}";
        }
    }

    @lines.push: '';
    @lines.append: q:to/END/.lines;

**DECISION TREE — follow this order:**

1. **FIRST, check if ANY existing worker can handle the task EXACTLY:**
   - web-browser: ONLY for fetching specific URLs (starts with http:// or https://). It CANNOT search.
   - web-search: For search queries (natural language, not URLs). Use for "search for X", "find information about Y".
   - system: For querying Camélia's own state — sessions, containers, health, config.
   - docker-compose: For managing Camélia containers and config files.
   - shell: LAST resort for simple bash commands. NOT for HTTP requests, NOT for web searches.

2. **If NO existing worker matches EXACTLY → use factory to create one.**
   Factory creates NEW worker types at runtime. This is the PREFERRED path for missing capabilities.
   Examples requiring factory:
   - "search the web for X" when web-search is not available → factory
   - "send an email to X" → factory to create email-sender
   - "post to Twitter" → factory to create twitter-poster

3. **Only use shell as fallback for SIMPLE operations** (file listing, echo, grep on local files).

**Conversational vs Action:**
Given a user request — and optionally conversation history — decide if it needs tool calls or can be answered directly.
If the request is conversational (greetings, chitchat, emotional support, follow-up questions that don't need external data), output an EMPTY array: `[]`

If the request requires action, decompose it into 1-3 INDEPENDENT subtasks.

Output ONLY a JSON array (empty or with subtask objects):
[]
[
  {
    "id": "task-1",
    "role": "brief role description",
    "worker_type": "<name from list above>",
    "task": "the action to perform",
    "args": {"key": "value"}
  }
]

Rules:
- The 'task' field MUST be an actionable value — command, URL, topic name, or description
- Use && to chain commands, ; for independent ones. Keep it under 500 chars
- Subtasks MUST be independent (no dependencies between them)
- Every subtask needs a "worker_type" field matching one of the available types
- If conversation history is provided, use it to maintain continuity
- **DISPATCH long-running commands** (sleep, builds, downloads, etc.) — do NOT try to run them yourself. The system handles progress notifications and follow-ups automatically.
- **Timer tasks MUST include args**: "duration_seconds" (Int) or "duration_minutes" (Num). Example: {"args":{"duration_seconds":30}} or {"args":{"duration_minutes":5}}
- **Timer notifications include the current date/time automatically.** For requests like "tell me the time in X" or "remind me in Y", just ONE timer subtask is enough — no separate time-fetching subtask needed. The timer notification will show: "🔔 Timer! _[message]_ 🕐 Hora atual: [datetime]"
- Output ONLY the JSON array, nothing else — no markdown fences, no explanations
END

    @lines.join("\n");
}

sub build-synth-prompt(Str $sid --> Str) {
    my @lines = q:to/END/.lines;
You are Camélia, a multi-agent AI assistant. Given the original request and worker results, combine them into a single coherent response. Be concise and direct. If there is previous conversation history, maintain continuity.
END

    @lines.push: "";
    @lines.push: "**Your capabilities (via workers):**";
    for %workers.values.sort({ .<name> }) -> %w {
        @lines.push: "- **{%w<name>}**: {%w<description>}";
    }
    @lines.push: "";
    @lines.push: "Key facts about this system:";
    @lines.push: "- You have a **factory** worker that can CREATE new worker types at runtime.";
    @lines.push: "- If a user asks for something no existing worker can do, suggest using the factory.";
    @lines.push: "- Never say 'I can't do that' without first checking if the factory could create a worker for it.";
    @lines.push: "- You run inside Camélia, a Raku multi-agent framework with NATS JetStream messaging.";
    @lines.push: "";
    @lines.push: "**Async notifications — YOU CAN ALWAYS REACH THE USER:**";
    @lines.push: "- For long-running tasks (shell commands, downloads, builds), the system automatically sends progress updates.";
    @lines.push: "- When a background task completes, the result is automatically delivered as a follow-up message.";
    @lines.push: "- **NEVER say 'I can't notify you when it's done'** — the system handles this. Just say 'I'll update you when it finishes.'";
    @lines.push: "- You are always connected — if a task is running, tell the user you'll follow up. You WILL follow up.";
    @lines.push: "";
    @lines.push: "Session ID: {$sid}";

    @lines.join("\n");
}

# ── Connect NATS ──

note "🟡 Orchestrator connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;

my $task-sub   = $nats.subscribe: 'orchestrator.task';
my $health-sub = $nats.subscribe: 'health.check.orchestrator';
my $registry-sub = $nats.subscribe: '$KV.WORKER_REGISTRY.>';
note "🟢 Orchestrator subscribed, entering react...";
my $task-chan = Channel.new;
my $task-consumer;  # module scope — acked in whenever $task-chan handler


# ── Inbox generation (BEFORE react — must initialize before event loop) ──
my @inbox-chars = |("a" .. "z"), |("A" .. "Z"), |("0" .. "9"), "_";
sub gen-inbox(--> Str) { "_INBOX." ~ (@inbox-chars.pick xx 32).join }

react {
    # ── JetStream setup: run concurrently so subscriptions are already tapped ──
    # Must run INSIDE react, otherwise Supplier loses messages during await
    start {
        setup-jetstream();
    }

    whenever $task-chan -> $msg {
        next unless $msg.payload;

        my $parsed = try from-json($msg.payload);
        if $! || !$parsed {
            note "⚠️ Invalid JSON in task message";
            $task-consumer.ack($msg) if $task-consumer;
            next;
        }
        my %req = $parsed;

        my $prompt     = %req<prompt>     // '';
        my $session-id = %req<session_id> // '';
        my $chat-id    = %req<chat_id>    // '';
        # reply_to from PAYLOAD (survives JetStream), fallback to NATS header
        my $reply-to   = %req<reply_to>   // $msg.?reply-to // '';

        unless $prompt {
            note "⚠️ orchestrator.task without prompt, ignoring";
            next;
        }
        unless $reply-to {
            note "⚠️ orchestrator.task without reply_to, ignoring";
            next;
        }

        note "📨 New task: {$prompt.substr(0, 100)}..." ~
            ($session-id ?? " (session: $session-id)" !! "");

        # 🔒 ACK immediately — message validated, prevent redelivery loop
        $task-consumer.ack($msg) if $task-consumer;

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
                :registered_workers(%workers.elems),
            });
        }
    }

    # ── Worker registry: update %workers as workers register ──
    whenever $registry-sub.supply -> $msg {
        next unless $msg.payload;
        my %w = try from-json($msg.payload);
        next if $!;
        next unless %w<name>;
        my $name = %w<name>;
        %workers{$name} = %w;
        note "📋 Registry: +{$name} (%workers.elems() total: {%workers.keys.sort.join(', ')})";
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

# ── Status notification to user via reply_to (entry-telegram listens here) ──
sub notify-user(Str $reply-to, Str $chat-id, Str $message, Str :$edit_key) {
    return unless $reply-to && $chat-id && $message;
    my %payload = :text($message), :chat_id($chat-id), :parse_mode<markdown>;
    %payload<edit_key> = $edit_key if $edit_key;
    $nats.publish: $reply-to, to-json(%payload);
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
# JETSTREAM SETUP — per-type streams for reliable delivery
# ═════════════════════════════════════════════

my constant @WORKER-TYPES = <shell web-browser system timer>;
my %stream-names;  # worker_type → stream name
my %consumer-names; # worker_type → durable consumer name

sub setup-jetstream() {
    note "📦 Setting up JetStream streams (per worker type)...";

    for @WORKER-TYPES -> $type {
        my $stream-name = "WORKER_" ~ $type.uc.subst('-', '_');
        my $subject = "worker.{$type}.task.>";
        my $consumer-name = "worker-pool-{$type}";
        %stream-names{$type} = $stream-name;
        %consumer-names{$type} = $consumer-name;

        my $stream = Nats::Stream.new:
            :$nats,
            :name($stream-name),
            :subjects([$subject]),
            :retention<limits>,
            ;

        my $s-supply = $stream.create;
        my $s-msg = await $s-supply.Promise;
        note $s-msg ?? "  ✅ Stream {$stream-name} ({$subject})" !! "  ⚠️ Stream {$stream-name} failed";

        # Each worker will create its own ephemeral consumer.
        # Durable consumer for monitoring (spawner uses this).
        note "  📥 Creating durable consumer {$consumer-name}...";
        my $consumer-subject = "\$JS.API.CONSUMER.CREATE.{$stream-name}.{$consumer-name}";
        my $consumer-config = to-json({
            :stream_name($stream-name),
            :config{
                :durable_name($consumer-name),
                :ack_policy<explicit>,
                :deliver_policy<all>,
                :filter_subject($subject),
                :max_ack_pending(20),
                :ack_wait(60_000_000_000),  # 60s
                :replay_policy<instant>,
                :inactive_threshold(300_000_000_000),  # 5 min
            },
        });
        my $c-resp = $nats.request($consumer-subject, $consumer-config);
        my $c-msg = await $c-resp.Promise;
        note $c-msg ?? "  ✅ Consumer {$consumer-name}" !! "  ⚠️ Consumer {$consumer-name} failed";
    }

    note "✅ JetStream ready ({+@WORKER-TYPES} worker streams).";

    # ── Orchestrator own task stream + consumer → Channel ──
    my $ts = Nats::Stream.new:
        :$nats, :name<ORCHESTRATOR_TASKS>,
        :subjects(["orchestrator.task"]),
        :retention<limits>, :max-age(86_400_000_000_000),
        ;
    my $tss = $ts.create; my $tsm = await $tss.Promise;
    note $tsm ?? "  ✅ Stream ORCHESTRATOR_TASKS" !! "  ⚠️ ORCHESTRATOR_TASKS";

    $task-consumer = Nats::Consumer.new:
        :$nats, :name<orchestrator-main>, :stream<ORCHESTRATOR_TASKS>,
        :ack-policy<explicit>, :deliver-policy<all>,
        :filter-subject("orchestrator.task"),
        :max-ack-pending(10), :ack-wait(120), :replay-policy<instant>,
        ;
    my $tcs = $task-consumer.create-named;
    my $tcm = await $tcs.Promise;
    note ($tcm && $tcm.payload && !$tcm.payload.starts-with("-ERR"))
        ?? "  ✅ Consumer orchestrator-main → Channel"
        !! "  ⚠️ Consumer: {$tcm.?payload // "no response"}";

    # Continuous pull loop via Channel bridge (Jun 2026 fix)
    # :no-wait → supply completes after ONE batch → no new tasks received
    # :expires(30) → long poll, loop keeps pulling indefinitely
    start {
        loop {
            try {
                $task-consumer.msgs(:batch(5), :expires(30)).tap: -> $msg {
                    $task-chan.send($msg) if $msg.?payload;
                };
            }
            sleep 0.5;  # prevent tight loop on persistent failure
        }
    }
    note "  ✅ Consumer pull loop → Channel";

    # ── Worker Registry KV (persistent, survives restart) ──
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

    # Seed default workers (factory writes new ones)
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
                   :description("Sets timers and notifies user when they fire. Use for reminders, countdowns, 'tell me in X minutes'. Args: duration_minutes (Num) or duration_seconds (Int)"), :topics([]) },
        factory => { :name<factory>, :subject("worker.factory.request"),
                   :description("Creates new worker types from specifications"), :topics([]) },
    );
    for %defaults.kv -> $name, %meta {
        $nats.publish: "\$KV.WORKER_REGISTRY.{$name}", to-json(%meta);
        %workers{$name} = %meta;
        note "    📌 {$name}";
    }
    note "  ✅ Registry seeded ({+%defaults} workers).";
    note "🔄 Orchestrator task consumer ready.";
}

# ═════════════════════════════════════════════
# REQUEST-REPLY — uses $nats.request (native nats.raku)
# ═════════════════════════════════════════════

# ── Manual inbox pattern (avoids nats.raku $nats.request race condition) ──
# Pitfall #17: $nats.request() published BEFORE the Supply was tapped.
# In threaded context (start {}), reply could arrive before .Promise was called,
# and the un-tapped head(1) Supply would drop the value forever.
# Fix: subscribe + tap Promise BEFORE publish — no window for the reply to be lost.
sub request-reply(Str $subject, Str $payload, Int :$timeout = 120 --> Hash) {
    my $inbox  = gen-inbox();
    my $sub    = $nats.subscribe: $inbox, :max-messages(1);
    my $p      = $sub.supply.head.Promise;  # TAP BEFORE publish — no race window
    $nats.publish: $subject, $payload, :reply-to($inbox);

    await Promise.anyof: $p, Promise.in($timeout);
    $nats.unsubscribe: $sub.sid;

    my %result = do if $p.so {
        my $msg = $p.result;
        if $msg && $msg.payload {
            try from-json($msg.payload) // { :error("JSON parse fail: $!") };
        } else {
            { :error("Empty response") };
        }
    } else {
        { :error("No response from {$subject}") };
    }

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

    stream($sid, 'done',
        :message("Direct response ready"),
        :result($final),
        :subtask_count(0),
    );

    note "✅ Direct response sent (session {$sid}).";
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

    # Notify user that we're working on it
    # notify-user($reply-to, $chat-id, "📋 *Analisando:* _{$prompt.substr(0, 100)}..._");

    # ═══════ STEP 1: Decompose (with history from session-store) ═══════
    note "📋 Decomposing (session {$sid}, task #{$task-n})...";
    my $decomp-prompt = build-decomp-prompt();
    my @decomp-msgs = (
        { :role<system>, :content($decomp-prompt) },
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
    if $! {
        my $err = "Failed to parse subtasks: $!";
        $nats.publish: $reply-to, to-json({ :error($err), :chat_id($chat-id) });
        stream($sid, 'error', :message($err));
        return;
    }

    # Empty subtasks = conversational, no tools needed — go direct to synthesis
    if @subtasks.elems == 0 {
        note "💬 Conversational — no subtasks, direct synthesis";
        stream($sid, 'decomposed', :message("Conversational — direct response"), :subtask_count(0));
        synthesize-and-respond($prompt, $reply-to, $sid, $seq, $chat-id, @history, []);
        $tasks-done++;
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

    # Notify user what we're doing
    my @worker-names = @subtasks.map({ $_<worker_type> // '?' }).unique;
    my $has-timer = @subtasks.first({ $_<worker_type> eq "timer" });
    my $edit-key = $has-timer ?? "timer-{" ~ $has-timer<id> ~ "}" !! Str;
    notify-user($reply-to, $chat-id, "⏳ *Processando...* ({+@subtasks} subtarefas)", :edit_key($edit-key));

    # ═══════ STEP 2: Publish tasks to JetStream-backed worker streams ═══════
    # Spawner independently monitors streams and spawns workers as needed.
    # No need to wait for workers — JetStream buffers messages.
    note "📤 Publishing {+@subtasks} tasks to worker streams...";

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

        # Route to typed worker stream: worker.<type>.task.<id>
        my $subject = $wtype eq 'factory'
            ?? 'worker.factory.request'
            !! "worker.{$wtype}.task.{$task-id}";

        my %payload = :id($task-id), :role($st<role>), :task($st<task>),
                      :session-id($sid), :reply_to($result-inbox);

        # Pass args for system tasks (e.g., container_detail needs name)
        %payload<args> = $st<args> if $st<args>:exists;

        # Timer worker needs user reply info for async notifications
        if $wtype eq 'timer' {
            %payload<user_reply_to> = $reply-to;
            %payload<user_chat_id>  = $chat-id;
        }

        # Factory requests need different payload format
        if $wtype eq 'factory' {
            %payload = :prompt($st<task>), :spec({ :name($task-id), :description($st<role>) }), :reply_to($result-inbox);
        }

        # Publish to JetStream-backed subject — worker pulls from stream
        $nats.publish: $subject, to-json(%payload), :reply-to($result-inbox);

        note "  📤 {$task-id} ({$wtype}) → {$subject} (reply: {$result-inbox})";
    }

    # ═══════ STEP 3: Collect results with progress notifications ═══════
    note "⏳ Waiting for {+@result-promises} worker result(s) (300s timeout)...";
    my $total-timeout  = 300;
    my $poll-interval  = 15;
    my $notify-every   = 30;
    my $last-notify    = 0;
    my $all = Promise.allof(@result-promises);
    my $elapsed = 0;

    while $elapsed < $total-timeout && !$all.so {
        await Promise.anyof: $all, Promise.in($poll-interval);
        $elapsed += $poll-interval;

        if !$all.so && $elapsed < $total-timeout {
            my $done = @result-promises.grep(*.so).elems;
            if $elapsed - $last-notify >= $notify-every && $done > 0 {
                notify-user($reply-to, $chat-id,
                    "⏳ *Ainda processando...* ({$done}/{+@result-promises} workers completos, {$elapsed}s)");
                $last-notify = $elapsed;
            } elsif $elapsed - $last-notify >= $notify-every {
                notify-user($reply-to, $chat-id,
                    "⏳ *Aguardando workers...* ({$elapsed}s)");
                $last-notify = $elapsed;
            }
        }
    }

    my @results;
    my $done-count = 0;
    if $all.so {
        @results = @result-promises.map(*.result);
    } else {
        # Partial — collect whatever resolved
        note "⏰ Timeout after {$elapsed}s — collecting partial results";
        for @result-promises -> $p {
            if $p.so {
                @results.push: $p.result;
            } else {
                @results.push: { :error("Worker result timeout after {$elapsed}s") };
            }
        }
        notify-user($reply-to, $chat-id,
            "⚠️ *Timeout após {$elapsed}s* — coletando resultados parciais...");
    }

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

    # ═══════ STEP 4b: Smart retry — analyze failures, try alternative approaches ═══════
    # Only retry if some workers succeeded (so system is alive) but some failed
    if $done-count > 0 && $done-count < +@results && +@results <= 3 {
        my $retry-attempt = 0;
        my $max-smart-retries = 2;

        while $retry-attempt < $max-smart-retries && $done-count < +@results {
            $retry-attempt++;
            note "🔄 Smart retry #{$retry-attempt} — analyzing failures...";

            # Build error context
            my $error-context = '';
            for @results.kv -> $i, $r {
                next unless $r<error>;
                $error-context ~= "Task {$i}: FAILED — {$r<error>}\n";
            }

            # Ask LLM for alternative approaches
            my @retry-msgs = (
                { :role<system>, :content(q:to/RETRY/) },
You are a task repair agent. Some workers failed. Analyze the errors and create
alternative subtasks to accomplish the original goal. Consider:
- Using a different worker type (web-browser instead of shell for HTTP)
- Breaking the task into smaller steps
- Using a different URL or approach
- Adding error handling (--fail, -L, retries)

Output ONLY a JSON array of new subtask objects, or [] if no fix is possible.
RETRY
            );
            @retry-msgs.push: { :role<user>, :content(
                "Original request: $prompt\n\n" ~
                "Failed tasks:\n{$error-context}\n\n" ~
                "Create alternative subtasks to recover."
            )};

            my %rr-resp = call-model(@retry-msgs, :temperature(0.3));
            next if %rr-resp<error>;

            my $rr-raw = %rr-resp<choices>[0]<message><content> // '';
            $rr-raw ~~ s/^ .*? '['/[/;
            $rr-raw ~~ s/']' .*? $/]/;
            my @retry-tasks = try from-json($rr-raw);
            next if $! || @retry-tasks.elems == 0;

            note "  💡 Generated {@retry-tasks.elems} alternative subtask(s)";

            # Execute retry tasks
            my @retry-promises;
            for @retry-tasks -> $st {
                my $task-id = $st<id> // ('retry-' ~ $retry-attempt ~ '-' ~ (^1000).pick);
                my $wtype   = $st<worker_type> // 'shell';
                my $result-inbox = "_INBOX.smart-retry.{$task-id}." ~ (('a'..'z').pick xx 8).join;

                my $result-sub = $nats.subscribe: $result-inbox, :1max-messages;
                my $result-promise = $result-sub.supply.head.Promise.then: -> $p {
                    my $msg = $p.result;
                    return { :error("Retry {$task-id}: no response") } unless $msg && $msg.payload;
                    try from-json($msg.payload) // { :error("Retry {$task-id}: Bad JSON") };
                };
                @retry-promises.push: $result-promise;

                my $subject = $wtype eq 'factory'
                    ?? 'worker.factory.request'
                    !! "worker.{$wtype}.task.{$task-id}";
                my %payload = :id($task-id), :role("retry-{$retry-attempt}-" ~ ($st<role> // '')), :task($st<task>), :session-id($sid);
                %payload<args> = $st<args> if $st<args>:exists;
                $nats.publish: $subject, to-json(%payload), :reply-to($result-inbox);
                note "  🔄 Smart retry {$task-id} ({$wtype}) → {$subject}: {$st<task>.substr(0, 80)}";
            }

            my $retry-all = Promise.allof(@retry-promises);
            await Promise.anyof: $retry-all, Promise.in(30);
            my @retry-results = $retry-all.so
                ?? @retry-promises.map(*.result)
                !! [{ :error("Smart retry timeout") },];

            my $retry-ok = 0;
            for @retry-results -> $r {
                if $r<error> {
                    note "    ❌ retry: {$r<error>}";
                } else {
                    $retry-ok++;
                    @results.push: $r;
                }
            }
            $done-count += $retry-ok;
            note "  ✅ Smart retry #{$retry-attempt}: {$retry-ok}/{+@retry-tasks} recovered, total done: {$done-count}/{+@results}";
        }
    }

    # ═══════ STEP 5: If no workers responded, graceful degradation (no retry) ═══════
    # Spawner already confirmed workers were ready. If they still don't respond,
    # something is fundamentally broken — don't loop, just degrade gracefully.
    if $done-count == 0 {
        note "⚠️ No workers responded despite spawner confirmation — graceful degradation";
        my $err-msg = "Unable to process your request right now — workers are not responding. Please try again.";
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
        stream($sid, 'degraded', :message("0/{+@subtasks} workers completed — graceful degradation"));
        note "✅ Graceful degradation response sent (session {$sid}).";
        $tasks-done++;
        return;
    }


    # ═══════ STEP 6: Synthesize (skip for timer-only — notification IS the response) ═══════
    my $timer-only = @subtasks.elems > 0 && @subtasks.all({ $_<worker_type> eq "timer" });
    if $timer-only {
        note "⏱️ Timer-only — skipping synthesis, notification will be sent by timer worker";
        session-append-batch($sid, $seq, [
            { :role<user>,      :content($prompt) },
            { :role<assistant>, :content("⏱️ Timer set. Notification incoming...") },
        ]);
        stream($sid, "done",
            :message("Timer dispatched — awaiting notification"),
            :subtask_count(+@subtasks),
        );
        note "✅ Timer task dispatched (session {$sid}).";
        $tasks-done++;
        return;
    }

    note "🧠 Synthesizing...";
    stream($sid, 'synthesizing', :message("Synthesizing final response..."));

    my $results-block = '';
    for @results.kv -> $i, $r {
        my $label = $r<worker-id> // "worker-{$i}";
        $results-block ~= "=== {$label} ({$r<role> // '?'}) ===\n";
        $results-block ~= to-json $r ~ "\n\n";
    }

    my @synth-msgs = (
        { :role<system>, :content(build-synth-prompt($sid)) },
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
