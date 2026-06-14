#!/usr/bin/env raku
# 🌺 Camélia PoC #6 — Worker Pool Spawner (Docker REST API + GC)
#
# Long-running service with Docker socket access.
# Manages worker containers via Docker REST API (curl).
# All communication via NATS (subscribe: spawner.control).
# NEW: periodic GC — cleans up zombie/idle worker containers.

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url     = %*ENV<NATS_URL>     // 'nats://127.0.0.1:4222';
my $max-workers  = %*ENV<MAX_WORKERS>   // 5;
my $worker-image  = %*ENV<WORKER_IMAGE>   // 'camelia-worker:latest';
my $docker-sock   = %*ENV<DOCKER_SOCK>    // '/var/run/docker.sock';
my $model-subject = %*ENV<MODEL_SUBJECT>  // 'model.deepseek.completion';

# ── Worker pool state ──
my %active-workers;  # container-id → { name }
my $next-id = 1;

# ── Connect NATS ──
note "🟡 Spawner connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 Spawner connected.";

my $sub = $nats.subscribe: 'spawner.control';
note "🟢 Listening on spawner.control (max_workers=$max-workers)...";

# Health check
my $health-sub = $nats.subscribe: 'health.check.spawner';

# ── Pipe NATS messages to channel ──
my $chan = Channel.new;
$sub.supply.tap: -> $msg {
    $chan.send($msg) if $msg.payload;
}

# ── Main processing loop ──
react {
    whenever $chan -> $msg {
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
            when 'ensure' {
                my $count = %req<count> // 0;
                handle-ensure($count, $reply-to);
            }
            when 'status' {
                handle-status($reply-to);
            }
            when 'stop_all' {
                handle-stop-all($reply-to);
            }
            when 'ensure_typed' {
                my $type = %req<type> // '';
                handle-ensure-typed($type, $reply-to);
            }
            default {
                $nats.publish: $reply-to, to-json({ :error("Unknown action: $action") });
            }
        }
    }

    # ═══════ GC: periodic zombie cleanup (every 60s) ═══════
    whenever Supply.interval(60) {
        handle-gc();
    }

    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({ :status<ok>, :service<spawner> });
        }
    }
}

# ═════════════════════════════════════════════
# DOCKER REST API HELPERS
# ═════════════════════════════════════════════

sub docker-api(Str $method, Str $path, Str $body?) {
    my $url = "http://localhost" ~ $path;
    my $tmpfile = $body ?? "/tmp/docker-api-{(^10000).pick}.json" !! Nil;
    if $tmpfile {
        spurt $tmpfile, $body;
        END { unlink $tmpfile if $tmpfile.IO.e }
    }

    my @args = ('curl', '-s', '--unix-socket', $docker-sock, '-X', $method,
                '-H', 'Content-Type: application/json');
    @args.push: '-d', '@' ~ $tmpfile if $tmpfile;
    @args.push: $url;

    my $proc = Proc::Async.new(|@args);
    my $output = '';
    $proc.stdout.lines(:chomp).tap(-> $line { $output ~= $line ~ "\n" });
    my $result = await $proc.start;
    my $exit   = $result.exitcode;

    if $exit != 0 {
        note "  ❌ Docker API error (exit={$exit}): {$output.substr(0, 200)}";
        return { :error("Docker API exit={$exit}") };
    }

    # Docker API often returns empty on success (201/204)
    return { :ok(True) } unless $output.trim;

    try from-json($output) // { :error("Invalid JSON from Docker API") };
}

# ═════════════════════════════════════════════
# HANDLERS
# ═════════════════════════════════════════════

sub handle-status(Str $reply-to) {
    $nats.publish: $reply-to, to-json({
        :active(%active-workers.elems),
        :max($max-workers),
        :workers(%active-workers.values.map({ .<name> }).Array),
    });
}

sub handle-ensure(Int $desired, Str $reply-to) {
    my $current = %active-workers.elems;
    my $target  = min($desired, $max-workers.Int);

    note "📊 ensure: current=$current desired=$desired target=$target";

    if $current >= $target {
        $nats.publish: $reply-to, to-json({
            :ok(True),
            :$current,
            :$target,
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

        # Step 1: Create container
        my $create-path = '/containers/create';
        my %create = docker-api('POST', $create-path, $container-config);
        if %create<error> {
            note "  ❌ Create failed: {%create<error>}";
            next;
        }
        my $cid = %create<Id> // '';
        unless $cid {
            note "  ❌ No container ID returned: {to-json(%create).substr(0, 200)}";
            next;
        }

        # Step 2: Start container
        my %start = docker-api('POST', "/containers/{$cid}/start");
        if %start<error> {
            note "  ❌ Start failed: {%start<error>}";
            docker-api('DELETE', "/containers/{$cid}");  # clean up
            next;
        }

        %active-workers{$cid} = { :name($worker-name) };
        @started.push: $worker-name;
        note "  ✅ {$worker-name} ({$cid})";
    }

    $nats.publish: $reply-to, to-json({
        :ok(@started.elems > 0),
        :workers(@started),
        :current(+%active-workers),
        :$target,
        :message("Started {@started.elems} of {$to-start} workers"),
    });
}

sub handle-stop-all(Str $reply-to) {
    my @stopped;
    for %active-workers.kv -> $cid, $info {
        note "  🛑 Stopping {$info<name>} ($cid)...";
        docker-api('POST', "/containers/{$cid}/stop");
        docker-api('DELETE', "/containers/{$cid}");
        @stopped.push: $info<name>;
    }
    %active-workers = ();
    $nats.publish: $reply-to, to-json({
        :ok(True),
        :stopped(@stopped),
    });
}

# ═════════════════════════════════════════════
# WORKER GC — cleans up zombie/idle containers
# ═════════════════════════════════════════════

sub handle-ensure-typed(Str $type, Str $reply-to) {
    my $image-name = "camelia-worker-{$type}:latest";
    my $container-name = "camelia-worker-{$type}";

    note "🔍 ensure_typed: type={$type} image={$image-name}";

    # Step 1: Check if already running — list all containers, filter by name
    my $list = docker-api('GET', '/containers/json?all=true');
    my $already-running = False;
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

    # Step 2: Check if image exists
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

    # Step 3: Build env vars — include model-specific keys for model types
    note "  🐳 Starting {$container-name}...";
    my @env = (
        "NATS_URL=nats://nats:4222",
        "SERVICE_NAME=worker-{$type}",
    );

    # Model types need API keys injected
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
        # Extract model name from type: model.ollama.qwen2.5-3b → qwen2.5:3b
        my $model-name = $type.subst(/^ 'model.ollama.' /, '').subst('-', ':', :g);
        @env.push: "OLLAMA_MODEL={$model-name}" if $model-name;
    }

    my %host-config = :NetworkMode<camelia_camelia>;

    # System worker needs Docker socket
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

    %active-workers{$cid} = { :name($container-name) };
    note "  ✅ {$container-name} ({$cid}) started";
    $nats.publish: $reply-to, to-json({ :ok(True), :container($container-name), :$cid, :status<started> });
}

# ═════════════════════════════════════════════
# WORKER GC — cleans up zombie/idle containers
# ═════════════════════════════════════════════

sub handle-gc() {
    # List ALL containers (including exited)
    my $list = docker-api('GET', '/containers/json?all=true');
    return if $list ~~ Hash && $list<error>;
    return unless $list ~~ Array;

    my @containers = $list.List;  # JSON array → Raku list
    return unless @containers.elems;

    my $now = now.Int;
    my $max-age = 900;  # 15 minutes max lifetime for a worker

    my ($zombies, $pruned) = (0, 0);

    for @containers -> $c {
        my @names = ($c<Names> // []).List;
        my $is-worker = False;
        for @names -> $n {
            if $n.starts-with('/camelia-worker') {
                $is-worker = True;
                last;
            }
        }
        next unless $is-worker;

        my $cid    = $c<Id> // '';
        my $state  = $c<State> // '';
        my $created = $c<Created> // 0;  # Unix timestamp

        # Remove exited/dead containers immediately
        if $state eq 'exited' | 'dead' {
            note "  🧹 GC: removing dead worker {$cid} (state={$state})";
            docker-api('DELETE', "/containers/{$cid}");
            %active-workers{$cid}:delete;
            $zombies++;
            next;
        }

        # Kill running workers that exceed max lifetime
        if $state eq 'running' && $created > 0 {
            my $age = $now - $created;
            if $age > $max-age {
                note "  🧹 GC: killing stale worker {$cid} (age={$age}s, max={$max-age}s)";
                docker-api('POST', "/containers/{$cid}/stop");
                docker-api('DELETE', "/containers/{$cid}");
                %active-workers{$cid}:delete;
                $pruned++;
            }
        }
    }

    # Prune active-workers entries for containers that no longer exist
    for %active-workers.keys -> $cid {
        my $found = False;
        for @containers -> $c {
            if ($c<Id> // '') eq $cid { $found = True; last }
        }
        unless $found {
            note "  🧹 GC: pruning stale tracking entry {$cid}";
            %active-workers{$cid}:delete;
            $pruned++;
        }
    }

    if $zombies || $pruned {
        note "🧹 GC done: {$zombies} zombies, {$pruned} pruned, {%active-workers.elems} active";
    }
}
