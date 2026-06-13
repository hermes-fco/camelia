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
my $worker-image = %*ENV<WORKER_IMAGE>  // 'camelia-worker:latest';
my $docker-sock  = %*ENV<DOCKER_SOCK>   // '/var/run/docker.sock';

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
            default {
                $nats.publish: $reply-to, to-json({ :error("Unknown action: $action") });
            }
        }
    }

    # ═══════ GC: periodic zombie cleanup (every 60s) ═══════
    whenever Supply.interval(60) {
        handle-gc();
    }
}

# ═════════════════════════════════════════════
# DOCKER REST API HELPERS
# ═════════════════════════════════════════════

sub docker-api(Str $method, Str $path, Str $body? --> Hash) {
    my $url = "http://localhost" ~ $path;
    my $cmd = "curl -s --unix-socket {$docker-sock} -X {$method}"
            ~ " -H 'Content-Type: application/json'";
    $cmd ~= " -d '{shell-escape($body)}'" if $body;
    $cmd ~= " {$url}";

    my $proc = shell($cmd, :out, :err);
    my $output = $proc.out.slurp(:close);
    my $exit   = $proc.exitcode;

    if $exit != 0 {
        note "  ❌ Docker API error (exit={$exit}): {$output.substr(0, 200)}";
        return { :error("Docker API exit={$exit}") };
    }

    # Docker API often returns empty on success (201/204)
    return { :ok(True) } unless $output.trim;

    try from-json($output) // { :error("Invalid JSON from Docker API") };
}

sub shell-escape(Str $s --> Str) {
    $s.subst(/\\/, '\\\\', :g).subst(/"/, '\\"', :g)
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
            :Env(["NATS_URL=nats://camelia-nats:4222"]),
            HostConfig => { :NetworkMode<camelia> },
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

sub handle-gc() {
    # List ALL containers (including exited)
    my %list = docker-api('GET', '/containers/json?all=true');
    return if %list<error>;

    my @containers = %list.List;  # JSON array → Raku list
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
