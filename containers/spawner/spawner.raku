#!/usr/bin/env raku
# 🌺 Camélia PoC #6 — Worker Pool Spawner (Docker REST API + reactive GC)
#
# Long-running service with Docker socket access.
# Manages worker containers via Docker REST API (curl).
# REACTIVE architecture:
#   - Subscribes to worker.status.> for lifecycle events (started/busy/idle)
#   - GC driven by events, not polling — idle > 15 min → kill
#   - Stream monitor: when worker reports idle + pending messages → spawn

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url      = %*ENV<NATS_URL>      // 'nats://127.0.0.1:4222';
my $max-workers   = %*ENV<MAX_WORKERS>   // 5;
my $worker-image   = %*ENV<WORKER_IMAGE>   // 'camelia-worker:latest';
my $docker-sock    = %*ENV<DOCKER_SOCK>    // '/var/run/docker.sock';
my $model-subject  = %*ENV<MODEL_SUBJECT>  // 'model.deepseek.completion';

# ── Worker tracking (event-driven) ──
# %worker-state{$worker-id} = { type, last-event, last-seen, container-id }
my %worker-state;
my $next-id = 1;

# ── Connect NATS ──
note "🟡 Spawner connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 Spawner connected.";

my $sub = $nats.subscribe: 'spawner.control';
note "🟢 Listening on spawner.control (max_workers=$max-workers)...";

# ── Reactive worker lifecycle subscription ──
my $worker-status-sub = $nats.subscribe: 'worker.status.>';
note "🟢 Listening on worker.status.>";

# Health check
my $health-sub = $nats.subscribe: 'health.check.spawner';

# ═════════════════════════════════════════════
# DOCKER REST API HELPERS
# ═════════════════════════════════════════════

sub docker-api(Str $method, Str $path, Str $body?) {
    my $url = "http://localhost" ~ $path;
    my $tmpfile = $body ?? "/tmp/docker-api-{(^10000).pick}.json" !! Nil;
    if $tmpfile {
        spurt $tmpfile, $body;
    }

    my @args = ('curl', '-s', '--max-time', '10', '--unix-socket', $docker-sock, '-X', $method,
                '-H', 'Content-Type: application/json');
    @args.push: '-d', '@' ~ $tmpfile if $tmpfile;
    @args.push: $url;

    my $proc = Proc::Async.new(|@args);
    my $output = '';
    $proc.stdout.lines(:chomp).tap(-> $line { $output ~= $line ~ "\n" });
    my $result = await $proc.start;
    my $exit   = $result.exitcode;

    # Clean up temp file immediately
    unlink $tmpfile if $tmpfile && $tmpfile.IO.e;

    if $exit != 0 {
        note "  ❌ Docker API error (exit={$exit}): {$output.substr(0, 200)}";
        return { :error("Docker API exit={$exit}") };
    }

    return { :ok(True) } unless $output.trim;
    try from-json($output) // { :error("Invalid JSON from Docker API") };
}

# ── NATS helpers ──
sub nats-request(Str $subject, Str $payload, Int :$timeout = 10 --> Hash) {
    my $sub = $nats.subscribe: my $inbox = "_INBOX.sp." ~ (^1_000_000).pick, :1max-messages;
    my $p   = $sub.supply.head.Promise;
    $nats.publish: $subject, $payload, :reply-to($inbox);
    await Promise.anyof: $p, Promise.in($timeout);
    $nats.unsubscribe: $sub;
    return { :error("No response") } unless $p.so;
    my $msg = $p.result;
    return { :error("Empty response") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("JSON parse fail") };
}

# ═════════════════════════════════════════════
# HANDLERS
# ═════════════════════════════════════════════

sub handle-status(Str $reply-to) {
    $nats.publish: $reply-to, to-json({
        :active(%worker-state.elems),
        :max($max-workers),
        :workers(%worker-state.values.map({ .<type> ~ ':' ~ .<id> }).Array),
    });
}

sub handle-ensure(Int $desired, Str $reply-to) {
    my $current = %worker-state.elems;
    my $target  = min($desired, $max-workers.Int);

    note "📊 ensure: current=$current desired=$desired target=$target";

    if $current >= $target {
        $nats.publish: $reply-to, to-json({
            :ok(True), :$current, :$target,
            :message("Already have {$current} workers"),
        });
        return;
    }

    my $to-start = $target - $current;
    note "🚀 Starting {$to-start} worker(s)...";

    my @started;
    for ^$to-start {
        my $worker-name = "camelia-worker-{$next-id++}";
        note "  🐳 Creating container {$worker-name}...";

        my $container-config = to-json({
            :Image($worker-image),
            :Hostname($worker-name),
            :Env(["NATS_URL=nats://nats:4222", "MODEL_SUBJECT={$model-subject}"]),
            HostConfig => { :NetworkMode<camelia_camelia> },
        });

        my %create = docker-api('POST', '/containers/create', $container-config);
        if %create<error> {
            note "  ❌ Create failed: {%create<error>}";
            next;
        }
        my $cid = %create<Id> // '';
        unless $cid {
            note "  ❌ No container ID returned";
            next;
        }

        my %start = docker-api('POST', "/containers/{$cid}/start");
        if %start<error> {
            note "  ❌ Start failed: {%start<error>}";
            docker-api('DELETE', "/containers/{$cid}");
            next;
        }

        @started.push: $worker-name;
        note "  ✅ {$worker-name} ({$cid})";
    }

    $nats.publish: $reply-to, to-json({
        :ok(@started.elems > 0),
        :workers(@started),
        :current(+%worker-state),
        :$target,
        :message("Started {@started.elems} of {$to-start} workers"),
    });
}

sub handle-stop-all(Str $reply-to) {
    my @stopped;
    my $list = docker-api('GET', '/containers/json?all=true');
    if $list ~~ Array {
        for $list.List -> $c {
            my @names = ($c<Names> // []).List;
            for @names -> $n {
                if $n.starts-with('/camelia-worker') {
                    docker-api('POST', "/containers/{$c<Id>}/stop");
                    docker-api('DELETE', "/containers/{$c<Id>}");
                    @stopped.push: $n.subst(/^ '/'/, '');
                }
            }
        }
    }
    %worker-state = ();
    $nats.publish: $reply-to, to-json({ :ok(True), :stopped(@stopped) });
}

sub handle-ensure-typed(Str $type, Str $reply-to) {
    my $image-name = "camelia-worker-{$type}:latest";
    my $container-name = "camelia-worker-{$type}";

    note "🔍 ensure_typed: type={$type} image={$image-name}";

    # Check if already running
    my $list = docker-api('GET', '/containers/json?all=true');
    if $list ~~ Array {
        for $list.List -> $c {
            my @names = ($c<Names> // []).List;
            for @names -> $n {
                if $n.starts-with("/{$container-name}") && ($c<State> // '') eq 'running' {
                    note "  ✅ {$container-name} already running";
                    $nats.publish: $reply-to, to-json({ :ok(True), :container($container-name), :status<already_running> });
                    return;
                }
                if $n.starts-with("/{$container-name}") {
                    note "  🔄 {$container-name} exists but state={$c<State> // 'unknown'}, removing...";
                    docker-api('DELETE', "/containers/{$c<Id>}");
                }
            }
        }
    }

    # Check if image exists
    my $img-check = docker-api('GET', "/images/{$image-name}/json");
    if $img-check<error> {
        note "  ❌ Image {$image-name} not found";
        $nats.publish: $reply-to, to-json({
            :ok(False), :error("Image '{$image-name}' not found"),
            :reason<no_image>, :type($type),
            :suggestion("Build image or use worker-factory to create this type"),
        });
        return;
    }

    note "  🐳 Starting {$container-name}...";
    my @env = (
        "NATS_URL=nats://nats:4222",
        "SERVICE_NAME=worker-{$type}",
    );

    if $type eq 'model.deepseek' {
        my $api-key = %*ENV<DEEPSEEK_API_KEY> // '';
        if $api-key {
            @env.push: "DEEPSEEK_API_KEY={$api-key}";
            note "  🔑 Injecting DEEPSEEK_API_KEY";
        }
        my $model-name = %*ENV<DEEPSEEK_MODEL> // 'deepseek-v4-pro';
        @env.push: "DEEPSEEK_MODEL={$model-name}";
    }
    elsif $type.starts-with('model.ollama') {
        my $ollama-url = %*ENV<OLLAMA_URL> // 'http://ollama:11434';
        @env.push: "OLLAMA_URL={$ollama-url}";
        my $model-name = $type.subst(/^ 'model.ollama.' /, '').subst('-', ':', :g);
        @env.push: "OLLAMA_MODEL={$model-name}" if $model-name;
    }

    my %host-config = :NetworkMode<camelia_camelia>;
    if $type eq 'system' {
        %host-config<Binds> = ["/var/run/docker.sock:/var/run/docker.sock"];
    }

    my $container-config = to-json({
        :Image($image-name),
        :Hostname($container-name),
        :Env(@env),
        HostConfig => %host-config,
    });

    my %create = docker-api('POST', '/containers/create?name=' ~ $container-name, $container-config);
    if %create<error> {
        note "  ❌ Create failed: {%create<error>}";
        $nats.publish: $reply-to, to-json({ :ok(False), :error("Container create failed: {%create<error>}") });
        return;
    }
    my $cid = %create<Id> // '';
    unless $cid {
        note "  ❌ No container ID returned";
        $nats.publish: $reply-to, to-json({ :ok(False), :error("No container ID") });
        return;
    }

    my %start = docker-api('POST', "/containers/{$cid}/start");
    if %start<error> {
        note "  ❌ Start failed: {%start<error>}";
        docker-api('DELETE', "/containers/{$cid}");
        $nats.publish: $reply-to, to-json({ :ok(False), :error("Container start failed") });
        return;
    }

    note "  ✅ {$container-name} ({$cid}) started";

    # Verify container is stable
    note "  🔍 Verifying {$container-name} is stable...";
    sleep 3;
    my $inspect = docker-api('GET', "/containers/{$cid}/json");
    if $inspect<error> {
        note "  ❌ Cannot inspect {$cid}: {$inspect<error>}";
        $nats.publish: $reply-to, to-json({ :ok(False), :error("Container inspect failed") });
        return;
    }
    my $running-state = $inspect<State><Status> // '';
    if $running-state ne 'running' {
        note "  ❌ {$container-name} crashed on startup (state={$running-state})";
        docker-api('DELETE', "/containers/{$cid}");
        $nats.publish: $reply-to, to-json({ :ok(False), :error("Worker crashed on startup (state={$running-state})") });
        return;
    }
    note "  🟢 {$container-name} stable (state={$running-state})";
    $nats.publish: $reply-to, to-json({ :ok(True), :container($container-name), :$cid, :status<ready> });
}

# ═════════════════════════════════════════════
# REACTIVE WORKER LIFECYCLE HANDLER
# ═════════════════════════════════════════════
#
# Events arrive on: worker.status.<type>.<worker-id>.<event>
# Events: started, busy, idle
#
# Spawner tracks:
#   - When worker starts → record it
#   - When worker goes idle → check stream for pending → spawn if needed
#   - GC: if no event from worker in 15 minutes → kill

sub parse-lifecycle-subject(Str $subject --> Hash) {
    # worker.status.<type>.<id>.<event>
    my @parts = $subject.split('.');
    return {} unless @parts.elems >= 5;
    return {
        :type(@parts[2]),
        :id(@parts[3]),
        :event(@parts[4]),
    };
}

sub handle-worker-event(Str $subject, Str $payload) {
    my %info = parse-lifecycle-subject($subject);
    return unless %info<type> && %info<id> && %info<event>;

    my $worker-id = %info<id>;
    my $type      = %info<type>;
    my $event     = %info<event>;
    my $now       = now.Int;

    given $event {
        when 'started' {
            %worker-state{$worker-id} = {
                :$type, :$worker-id, :last-event($event), :last-seen($now), :container-id(''),
            };
            note "🟢 Worker {$type}:{$worker-id} started";
        }
        when 'busy' {
            if %worker-state{$worker-id} {
                %worker-state{$worker-id}<last-event> = 'busy';
                %worker-state{$worker-id}<last-seen>  = $now;
            } else {
                %worker-state{$worker-id} = {
                    :$type, :$worker-id, :last-event('busy'), :last-seen($now), :container-id(''),
                };
            }
        }
        when 'idle' {
            if %worker-state{$worker-id} {
                %worker-state{$worker-id}<last-event> = 'idle';
                %worker-state{$worker-id}<last-seen>  = $now;
            }
            # ── When worker goes idle, check if stream has pending messages ──
            # Don't await inside react whenever — use start {}
            start { check-and-spawn($type); }
        }
    }
}

# ═════════════════════════════════════════════
# STREAM-AWARE SPAWN — triggered when worker goes idle
# ═════════════════════════════════════════════

sub check-and-spawn(Str $type) {
    my $stream-name = "WORKER_" ~ $type.uc.subst('-', '_');

    # Query JetStream stream info
    my %info = nats-request("\$JS.API.STREAM.INFO.{$stream-name}", '', :timeout(5));
    return if %info<error>;

    my $pending = %info<state><messages> // 0;
    return if $pending == 0;

    # Check if we already have a running container of this type
    my $already = False;
    my $list = docker-api('GET', '/containers/json?all=true');
    if $list ~~ Array {
        my $container-name = "camelia-worker-{$type}";
        for $list.List -> $c {
            my @names = ($c<Names> // []).List;
            for @names -> $n {
                if $n.starts-with("/{$container-name}") && ($c<State> // '') eq 'running' {
                    $already = True;
                    last;
                }
            }
        }
    }

    unless $already {
        note "📊 Stream {$stream-name}: {$pending} pending, worker idle — spawning...";
        handle-ensure-typed($type, '');
    }
}

# ═════════════════════════════════════════════
# REACTIVE GC — driven by worker status events
# ═════════════════════════════════════════════

sub handle-reactive-gc() {
    my $now = now.Int;
    my $max-idle = 900;  # 15 minutes

    # Check worker-state for stale entries
    for %worker-state.keys -> $worker-id {
        my $state = %worker-state{$worker-id};
        my $idle-seconds = $now - $state<last-seen>;

        if $idle-seconds > $max-idle && $state<last-event> ne 'busy' {
            note "🧹 GC: {$state<type>}:{$worker-id} idle for {$idle-seconds}s (> {$max-idle}s) — killing...";
            kill-worker-by-type-id($state<type>, $worker-id);
            %worker-state{$worker-id}:delete;
        }
    }

    # Prune dead containers (zombies)
    my $list = docker-api('GET', '/containers/json?all=true');
    return unless $list ~~ Array;

    for $list.List -> $c {
        my @names = ($c<Names> // []).List;
        my $is-worker = False;
        for @names -> $n {
            if $n.starts-with('/camelia-worker') {
                $is-worker = True;
                last;
            }
        }
        next unless $is-worker;

        my $cid   = $c<Id> // '';
        my $state = $c<State> // '';
        if $state eq 'exited' | 'dead' {
            note "  🧹 GC: removing zombie {$cid} (state={$state})";
            docker-api('DELETE', "/containers/{$cid}");
        }
    }

    # Prune stale worker-state entries (workers that vanished without saying goodbye)
    my %running-containers;
    for $list.List -> $c {
        my @names = ($c<Names> // []).List;
        for @names -> $n {
            if $n.starts-with('/camelia-worker') {
                %running-containers{$c<Id>} = True;
            }
        }
    }
    for %worker-state.keys -> $worker-id {
        my %state = %worker-state{$worker-id};
        next unless %state<container-id>;
        unless %running-containers{%state<container-id>} {
            note "  🧹 GC: pruning stale state entry for {$worker-id}";
            %worker-state{$worker-id}:delete;
        }
    }
}

sub kill-worker-by-type-id(Str $type, Str $worker-id) {
    my $container-name = "camelia-worker-{$type}";
    my $list = docker-api('GET', '/containers/json?all=true');
    return unless $list ~~ Array;

    for $list.List -> $c {
        my @names = ($c<Names> // []).List;
        for @names -> $n {
            if $n.starts-with("/{$container-name}") {
                note "  🛑 Killing {$n}";
                docker-api('POST', "/containers/{$c<Id>}/stop");
                docker-api('DELETE', "/containers/{$c<Id>}");
                return;
            }
        }
    }
}

# ═════════════════════════════════════════════
# MAIN REACT LOOP — event-driven, NO Supply.interval
# ═════════════════════════════════════════════

note "🔄 Spawner react loop ready";

react {
    # ── Spawner control messages ──
    whenever $sub.supply -> $msg {
        my $reply-to = $msg.?reply-to;
        unless $reply-to {
            note "⚠️ No reply-to, ignoring";
            next;
        }

        my %req = try from-json($msg.payload);
        if $! {
            $nats.publish: $reply-to, to-json({ :error("Invalid JSON") });
            next;
        }

        my $action = %req<action> // '';
        given $action {
            when 'ensure'       { handle-ensure(%req<count> // 0, $reply-to) }
            when 'status'       { handle-status($reply-to) }
            when 'stop_all'     { handle-stop-all($reply-to) }
            when 'ensure_typed' { handle-ensure-typed(%req<type> // '', $reply-to) }
            default {
                $nats.publish: $reply-to, to-json({ :error("Unknown action: $action") });
            }
        }
    }

    # ── REACTIVE worker lifecycle — whenever worker.status.> ──
    whenever $worker-status-sub.supply -> $msg {
        next unless $msg.payload;
        handle-worker-event($msg.subject // '', $msg.payload);
    }

    # ── GC: periodic zombie cleanup (still needs occasional Docker API check) ──
    # Reduced from 60s to 120s — most GC is now event-driven via worker.status.>
    whenever Supply.interval(120) {
        try {
            handle-reactive-gc();
            CATCH { note "⚠️ GC error: {.message}" }
        }
    }

    # ── Health check ──
    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service<spawner>,
                :active_workers(%worker-state.elems),
                :workers(%worker-state.values.map({ .<type> ~ ':' ~ .<id> ~ ':' ~ .<last-event> }).Array),
            });
        }
    }
}
