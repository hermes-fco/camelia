#!/usr/bin/env raku
# 🌺 Camélia — Worker: System (container awareness)
#
# Subscribes to worker.system.task.> — answers predefined topics
# about the Camélia system: container status, health, uptime.
# Direct Docker access via socket (not through tool-executor).

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url     = %*ENV<NATS_URL>     // 'nats://127.0.0.1:4222';
my $service-name = %*ENV<SERVICE_NAME>  // 'worker-system';
my $docker-sock  = %*ENV<DOCKER_SOCK>   // '/var/run/docker.sock';

# ── Connect NATS ──
note "🟡 System-Worker connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 System-Worker connected.";

my $task-sub   = $nats.subscribe: 'worker.system.task.>';
my $health-sub = $nats.subscribe: 'health.check.worker.system';
note "🟢 Listening on worker.system.task.>";

# ── Docker query helper (via REST API, like spawner) ──
sub docker-api(Str $method, Str $path, Str $body?) {
    my $url = "http://localhost" ~ $path;
    my $tmpfile = $body ?? "/tmp/docker-sys-{(^10000).pick}.json" !! Nil;
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
    return { :error("Docker API exit={$result.exitcode}") } if $result.exitcode != 0;
    return { :ok(True) } unless $output.trim;
    try from-json($output) // { :error("Invalid JSON from Docker API") };
}

# ── Run shell command ──
sub run-cmd(Str $cmd --> Hash) {
    my $proc = Proc::Async.new('sh', '-c', $cmd);
    my ($out, $err) = ('', '');
    $proc.stdout.lines(:chomp).tap(-> $l { $out ~= $l ~ "\n" });
    $proc.stderr.lines(:chomp).tap(-> $l { $err ~= $l ~ "\n" });
    my $result = await $proc.start;
    return { :ok($result.exitcode == 0), :stdout($out.trim), :stderr($err.trim), :exit($result.exitcode) };
}

# ── Predefined topic handlers ──
sub handle-containers-list(--> Hash) {
    my $resp = docker-api('GET', '/containers/json?all=true');
    return { :error($resp<error>) } if $resp<error>;

    my @containers;
    for $resp.List -> $c {
        my @names = ($c<Names> // []).List.map({ S:g/^ '/'// });
        my $name  = @names[0] // $c<Id>.substr(0, 12);
        # Filter: only camelia containers
        next unless $name.starts-with('camelia-');
        @containers.push: {
            :$name,
            :state($c<State> // 'unknown'),
            :status($c<Status> // ''),
            :image(($c<Image> // '').substr(0, 40)),
            :created($c<Created> // 0),
        };
    }

    return { :ok(True), :containers(@containers), :count(+@containers) };
}

sub handle-container-detail(Str $name --> Hash) {
    # Find container by name
    my $list = docker-api('GET', '/containers/json?all=true');
    return { :error($list<error>) } if $list<error>;

    my $cid;
    for $list.List -> $c {
        my @names = ($c<Names> // []).List.map({ S:g/^ '/'// });
        if @names[0] && @names[0].starts-with($name) {
            $cid = $c<Id>;
            last;
        }
    }
    return { :error("Container '$name' not found") } unless $cid;

    # Get detailed inspect
    my $inspect = docker-api('GET', "/containers/{$cid}/json");
    return { :error($inspect<error>) } if $inspect<error>;

    my $state = $inspect<State> // {};
    my $config = $inspect<Config> // {};

    return {
        :ok(True),
        :$name,
        :id($cid),
        :state($state<Status> // 'unknown'),
        :started_at($state<StartedAt> // ''),
        :image(($config<Image> // '').substr(0, 50)),
        :env(((($config<Env> // []).List).grep({ not .starts-with('ENTRY_TOKEN='|'DEEPSEEK_API_KEY=') })).join(', ').substr(0, 300)),
    };
}

sub handle-system-health(--> Hash) {
    my %result = :ok(True), :containers([]);

    my $list = docker-api('GET', '/containers/json?all=true');
    return { :error($list<error>) } if $list<error>;

    my ($running, $stopped, $total) = (0, 0, 0);
    for $list.List -> $c {
        my @names = ($c<Names> // []).List.map({ S:g/^ '/'// });
        my $name = @names[0] // '';
        next unless $name.starts-with('camelia-');
        $total++;
        if ($c<State> // '') eq 'running' { $running++ } else { $stopped++ }
        %result<containers>.push: {
            :$name,
            :state($c<State> // 'unknown'),
            :status($c<Status> // ''),
        };
    }

    %result<total>   = $total;
    %result<running> = $running;
    %result<stopped> = $stopped;
    %result<healthy> = $running == $total && $total > 0;

    return %result;
}

# ── Session store bridge ──
sub session-request(Str $subject, Str $payload --> Hash) {
    my $supply = $nats.request: $subject, $payload;
    my $p = $supply.head.Promise;
    await Promise.anyof: $p, Promise.in(10);
    if $p.so {
        my $msg = $p.result;
        if $msg && $msg.payload {
            try from-json($msg.payload) // { :error("JSON parse fail: $!") }
        } else {
            { :error("Empty response from {$subject}") }
        }
    } else {
        { :error("No response from {$subject}") }
    }
}

sub handle-session-get(Str $session-id --> Hash) {
    return { :error("session_id required") } unless $session-id;
    session-request('session.store.get', to-json({ :session_id($session-id) }));
}

sub handle-session-list(--> Hash) {
    session-request('session.store.list', '{}');
}

# ── Reconfigure (env vars + restart) ──
sub handle-reconfigure(Str $container, %env --> Hash) {
    return { :error("container name required") } unless $container;

    # 1. Find container by name prefix
    my $list = docker-api('GET', '/containers/json?all=true');
    return { :error($list<error>) } if $list<error>;

    my ($cid, $existing-name);
    for $list.List -> $c {
        my @names = ($c<Names> // []).List.map({ S:g/^ '/'// });
        if @names[0] && @names[0].starts-with($container) {
            $cid = @names[0];
            $existing-name = @names[0];
            last;
        }
    }
    return { :error("Container '$container' not found") } unless $cid;

    # 2. Inspect current config
    my $inspect = docker-api('GET', "/containers/{$cid}/json");
    return { :error($inspect<error>) } if $inspect<error>;

    my $config = $inspect<Config> // {};
    my $host-config = $inspect<HostConfig> // {};
    my $state = $inspect<State> // {};

    # 3. Extract current env vars (as list of "KEY=VALUE")
    my @current-env = ($config<Env> // []).List.flat;

    # 4. Merge new env vars (update existing, append new)
    my %env-map;
    for @current-env -> $e {
        my ($k, $v) = $e.split('=', 2);
        %env-map{$k} = $v if $k;
    }
    for %env.kv -> $k, $v {
        %env-map{$k} = $v;
    }
    my @new-env = %env-map.map({ "{$_.key}={$_.value}" }).sort;

    my @changed = %env.keys.sort;
    note "🔄 Reconfiguring {$existing-name}: {@changed.join(', ')}";

    # 5. Extract current run config
    my $image = $config<Image> // '';
    my $network = ($host-config<NetworkMode> // '').Str;
    my $restart = ($host-config<RestartPolicy> // {}).<Name> // 'always';

    # 6. Stop + remove
    docker-api('POST', "/containers/{$cid}/stop?t=5");
    sleep 2;
    docker-api('DELETE', "/containers/{$cid}?force=true");

    # 7. Recreate with new env
    my $create-body = to-json({
        :Image($image),
        :Env(@new-env),
        :HostConfig({
            :NetworkMode($network),
            :RestartPolicy({ :Name($restart) }),
            :Binds($host-config<Binds> // []),
        }),
        :Cmd($config<Cmd> // []),
        :Entrypoint($config<Entrypoint> // []),
    });

    note "  📦 Creating new {$existing-name}...";
    my $create = docker-api('POST', "/containers/create?name={$existing-name}", $create-body);
    return { :error("create failed: {$create<error>}") } if $create<error>;

    my $new-cid = $create<Id> // '';
    return { :error("no container ID returned") } unless $new-cid;

    # 8. Start
    docker-api('POST', "/containers/{$new-cid}/start");

    note "  ✅ Reconfigured {$existing-name} (new: {$new-cid.substr(0, 12)}, changed: {@changed.join(', ')})";

    return {
        :ok(True),
        :container($existing-name),
        :new_id($new-cid.substr(0, 12)),
        :env_changed(@changed),
    };
}

# ── Topic router ──
sub dispatch(Str $topic, %args --> Hash) {
    given $topic {
        when 'containers_list'   { handle-containers-list() }
        when 'container_detail'  { handle-container-detail(%args<name> // '') }
        when 'system_health'     { handle-system-health() }
        when 'session_get'       { handle-session-get(%args<session_id> // '') }
        when 'session_list'      { handle-session-list() }
        when 'reconfigure'       { handle-reconfigure(%args<container> // '', %args<env> // {}) }
        default {
            { :error("Unknown topic: $topic. Available: containers_list, container_detail, system_health, session_get, session_list, reconfigure") }
        }
    }
}

# ── React loop ──
react {
    whenever $task-sub.supply -> $msg {
        next unless $msg.payload;
        my $reply-to = $msg.?reply-to;
        unless $reply-to {
            note "⚠️ No reply-to, ignoring";
            next;
        }

        my %task = try from-json($msg.payload);
        if $! {
            $nats.publish: $reply-to, to-json({ :ok(False), :error("Invalid JSON") });
            next;
        }

        my $topic = %task<topic> // %task<task> // '';
        my %args  = %task<arguments> // %task<args> // {};

        note "🔍 System query: {$topic}";
        start {
            my %resp := dispatch($topic, %args);
            %resp<worker> = $service-name;
            $nats.publish: $reply-to, to-json(%resp);
            if %resp<error> {
                note "  ❌ {$topic}: {%resp<error>}";
            } else {
                note "  ✅ {$topic} done";
            }
        }
    }

    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service<worker-system>,
                :topics(['containers_list', 'container_detail', 'system_health', 'session_get', 'session_list', 'reconfigure']),
            });
        }
    }
}
