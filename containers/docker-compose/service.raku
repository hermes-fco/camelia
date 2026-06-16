#!/usr/bin/env raku
# 🌺 Camélia — Worker: Docker Compose
#
# Manages Camélia containers via docker compose CLI.
# Subscribes to worker.docker-compose.task.>
#
# Commands (topic field):
#   up         — start a service             (args: service)
#   down       — stop a service              (args: service)
#   restart    — restart a service           (args: service)
#   rebuild    — rebuild + restart a service (args: service)
#   ps         — list running services
#   logs       — get logs for a service      (args: service, tail=50)
#   status     — health summary (all services)
#   pull       — pull latest images          (args: service, optional)
#
# The camelia project is mounted at /camelia (docker-compose.yml + containers/ + Dockerfiles).

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url     = %*ENV<NATS_URL>     // 'nats://127.0.0.1:4222';
my $service-name = %*ENV<SERVICE_NAME>  // 'worker-docker-compose';
my $project-dir  = %*ENV<PROJECT_DIR>   // '/camelia';
my $worker-id    = ('dc' ~ (^10000).pick).Str;

# ── Connect NATS ──
note "🟡 Docker-Compose-Worker-{$worker-id} connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 Docker-Compose-Worker-{$worker-id} connected. Project: {$project-dir}";

my $task-sub   = $nats.subscribe: 'worker.docker-compose.task.>';
my $health-sub = $nats.subscribe: 'health.check.worker.docker-compose';
note "🟢 Listening on worker.docker-compose.task.>";

# ── Lifecycle ──
sub lifecycle(Str $event) {
    $nats.publish: "worker.status.docker-compose.{$worker-id}.{$event}",
        to-json({ :$worker-id, :type<docker-compose>, :$event, :ts(now.Real) });
}
my $last-activity = now;

# ── Run docker compose command ──
sub compose(Str $cmd, :$timeout = 120 --> Hash) {
    my $full-cmd = "cd {$project-dir} && docker-compose -f docker-compose.yaml {$cmd} 2>&1";
    note "  🐳 {$full-cmd}";
    my $proc = Proc::Async.new('sh', '-c', $full-cmd);
    my ($out, $err) = ('', '');
    $proc.stdout.lines(:chomp).tap(-> $l { $out ~= $l ~ "\n" });
    $proc.stderr.lines(:chomp).tap(-> $l { $err ~= $l ~ "\n" });
    my $result = await $proc.start;
    my $ok = $result.exitcode == 0;
    note "  " ~ ($ok ?? "✅" !! "❌ exit={$result.exitcode}") ~ " {$cmd}";
    return { :$ok, :stdout($out.trim), :stderr($err.trim), :exit($result.exitcode) };
}

# ── Command handlers ──

sub cmd-up(Str $service --> Hash) {
    return { :error("service required") } unless $service;
    compose("up -d {$service}");
}

sub cmd-down(Str $service --> Hash) {
    return { :error("service required") } unless $service;
    compose("down {$service}");
}

sub cmd-restart(Str $service --> Hash) {
    return { :error("service required") } unless $service;
    compose("restart {$service}");
}

sub cmd-rebuild(Str $service --> Hash) {
    return { :error("service required") } unless $service;
    my %build = compose("build {$service}", :timeout(300));
    return %build unless %build<ok>;

    # Stop, rm, recreate with compose up -d
    compose("stop {$service}");
    compose("rm -f {$service}");
    compose("up -d {$service}");
}

sub cmd-ps(--> Hash) {
    compose("ps");
}

sub cmd-logs(Str $service, Int $tail = 50 --> Hash) {
    return { :error("service required") } unless $service;
    compose("logs --tail={$tail} {$service}");
}

sub cmd-status(--> Hash) {
    my %r = compose("ps");
    return %r unless %r<ok>;

    # docker-compose ps returns text table; parse it
    my @services;
    for %r<stdout>.lines -> $line {
        next if $line ~~ /^ \s* Name \s+ /;     # skip header
        next if $line ~~ /^ '-'+ $/;            # skip separator
        next unless $line.trim;
        # Format: "name   state   ..."
        my @cols = $line.split(/\s\s+/);
        if @cols.elems >= 2 {
            @services.push: {
                :name(@cols[0]),
                :state(@cols[1] // 'unknown'),
                :status(@cols.elems > 2 ?? @cols[2] !! ''),
            };
        }
    }

    my $running = @services.grep({ .<state> && .<state>.starts-with('Up') }).elems;
    return {
        :ok(True),
        :services(@services),
        :total(+@services),
        :$running,
        :healthy($running == +@services && +@services > 0),
    };
}

sub cmd-pull(Str $service? --> Hash) {
    my $target = $service ?? $service !! '';
    compose("pull {$target}", :timeout(300));
}

# ── Config file editing ──

sub cmd-get-config(--> Hash) {
    my $path = "{$project-dir}/docker-compose.yaml";
    return { :error("Config file not found: {$path}") } unless $path.IO.e;
    my $yaml = slurp $path;

    # Redact ALL environment variable values — keep the var name, replace value with ***
    # Matches lines like: "  - VAR=value" (compose env list format)
    my $redacted = $yaml;
    $redacted ~~ s:g/^^ (\s* '-' \s* \S+ '=') \S.* $$/$0***/;

    return { :ok(True), :yaml($redacted), :path($path), :size($yaml.chars) };
}

sub cmd-validate-config(Str $yaml --> Hash) {
    return { :error("yaml content required") } unless $yaml;

    # Write to temp file and validate with docker-compose config
    my $tmp = "/tmp/dc-validate-{(^10000).pick}.yaml";
    spurt $tmp, $yaml;
    my $proc = Proc::Async.new('docker-compose', '-f', $tmp, 'config');
    my ($out, $err) = ('', '');
    $proc.stdout.lines(:chomp).tap(-> $l { $out ~= $l ~ "\n" });
    $proc.stderr.lines(:chomp).tap(-> $l { $err ~= $l ~ "\n" });
    my $result = await $proc.start;
    unlink $tmp;

    if $result.exitcode == 0 {
        return { :ok(True), :valid(True), :normalized($out.trim) };
    } else {
        return { :ok(True), :valid(False), :error($err.trim) };
    }
}

sub cmd-set-config(Str $yaml --> Hash) {
    return { :error("yaml content required") } unless $yaml;

    my $path = "{$project-dir}/docker-compose.yaml";

    # Validate first
    my %validation = cmd-validate-config($yaml);
    return %validation unless %validation<valid>;

    # Backup existing
    if $path.IO.e {
        my $backup = "{$path}.bak." ~ DateTime.now.Str.subst(/<[\s\:]>/, '_', :g).substr(0, 19);
        copy $path, $backup;
        note "  📋 Backup: {$backup}";
    }

    # Write new config
    spurt $path, $yaml;
    note "  ✅ Config written ({$yaml.chars} bytes)";

    return { :ok(True), :written($yaml.chars), :$path };
}

# ── Topic router ──
sub dispatch(Str $topic, %args --> Hash) {
    given $topic {
        when 'up'              { cmd-up(%args<service> // '') }
        when 'down'            { cmd-down(%args<service> // '') }
        when 'restart'         { cmd-restart(%args<service> // '') }
        when 'rebuild'         { cmd-rebuild(%args<service> // '') }
        when 'ps'              { cmd-ps() }
        when 'logs'            { cmd-logs(%args<service> // '', (%args<tail> // 50).Int) }
        when 'status'          { cmd-status() }
        when 'pull'            { cmd-pull(%args<service>) }
        when 'get-config'      { cmd-get-config() }
        when 'set-config'      { cmd-set-config(%args<yaml> // %args<content> // '') }
        when 'validate-config' { cmd-validate-config(%args<yaml> // %args<content> // '') }
        default {
            { :error("Unknown command: {$topic}. Available: up, down, restart, rebuild, ps, logs, status, pull, get-config, set-config, validate-config") }
        }
    }
}

# ── React loop ──
react {
    start {
        sleep 0.5;
        lifecycle('started');
    }

    whenever $task-sub.supply -> $msg {
        next unless $msg.payload;
        my $reply-to = $msg.?reply-to;
        unless $reply-to {
            note "⚠️ No reply-to, ignoring";
            next;
        }

        my $parsed = try from-json($msg.payload);
        if $! || !$parsed {
            $nats.publish: $reply-to, to-json({ :error("Invalid JSON") });
            next;
        }
        %task = $parsed;

        my $topic = %task<topic> // %task<task> // '';
        my %args  = %task<arguments> // %task<args> // {};

        note "🐳 DC command: {$topic}" ~ (%args<service> ?? " (%args<service>)" !! "");
        $last-activity = now;
        lifecycle('busy');
        start {
            my %resp := dispatch($topic, %args);
            %resp<worker> = $service-name;
            $nats.publish: $reply-to, to-json(%resp);
            $last-activity = now;
            lifecycle('idle');
        }
    }

    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service<worker-docker-compose>,
                :$worker-id,
                :idle_seconds((now - $last-activity).Int),
                :commands(['up', 'down', 'restart', 'rebuild', 'ps', 'logs', 'status', 'pull']),
            });
        }
    }
}
