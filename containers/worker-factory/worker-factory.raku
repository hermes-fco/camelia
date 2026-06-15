#!/usr/bin/env raku
# 🌺 Camélia PoC #3 — Worker Factory (meta-worker)
#
# Creates new worker types from prompts.
# Subscribes to worker.factory.request — receives spec, generates code via model,
# validates with raku -c, persists to /workers/<type>/.
# Uses session-store for build memory (idempotency, recovery).

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url      = %*ENV<NATS_URL>       // 'nats://127.0.0.1:4222';
my $model-subject = %*ENV<MODEL_SUBJECT>   // 'model.deepseek.completion';
my $max-retries   = %*ENV<FACTORY_RETRIES> // 3;
my $worker-id     = ('fc' ~ (^1000).pick).Str;

# ── Load template ──
my $template-path = '/opt/camelia/templates/worker-api.raku';
my $template      = $template-path.IO.e ?? $template-path.IO.slurp !! '';

# ── System prompt for code generation ──
my $gen-system = q:to/END/;
You are a Raku code generator. Given a template with {{PLACEHOLDERS}} and a specification,
fill in the template with working Raku code.

Template placeholders:
  {{NAME}}         — worker name (e.g., 'api.github')
  {{DESCRIPTION}}  — short description
  {{BASE_URL}}     — API base URL
  {{SUBJECT}}      — NATS subscription subject (e.g., 'api.github.>')
  {{TOOLS_SCHEMA}} — Comment describing tools available (endpoints, params, return types)
  {{TOOL_LOGIC}}   — Raku sub handle-task(%task --> Hash) with given/when for each tool

Rules:
- Output ONLY the filled template, nothing else — no markdown fences, no explanations
- Use idiomatic Raku (colon pairs, Proc::Async, no shell())
- The handle-task sub receives %task with <action> key and optional params
- Return { :ok(True), :data(...) } on success, { :error("reason") } on failure
- Never expose API keys in the code — use constant API-KEY from environment
- Use Proc::Async for all HTTP calls
- For errors, return { :ok(False), :error("message") }
END

# ── Connect NATS ──
note "🟡 Factory connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 Factory connected.";

# ── Lifecycle events ──
my $lifecycle-subject = "worker.status.factory.{$worker-id}";
sub lifecycle(Str $event) {
    $nats.publish: "{$lifecycle-subject}.{$event}",
        to-json({ :$worker-id, :type<factory>, :$event, :ts(now.Real) });
}
my $task-sub   = $nats.subscribe: 'worker.factory.request';
my $health-sub = $nats.subscribe: 'health.check.worker.factory';
note "🟢 Listening on worker.factory.request";

# ── Track idle time ──
my $last-activity = now;

# ── Model call helper ──
sub call-model(@messages, :$temperature = 0.1 --> Hash) {
    my %body = :model('deepseek-v4-pro'), :@messages, :$temperature;
    my $sub = $nats.subscribe: my $inbox = "_INBOX.fa." ~ (^1_000_000).pick, :1max-messages;
    my $p   = $sub.supply.head.Promise;
    $nats.publish: $model-subject, to-json(%body), :reply-to($inbox);
    await Promise.anyof: $p, Promise.in(120);
    $nats.unsubscribe: $sub;
    return { :error("Model timeout") } unless $p.so;
    my $msg = $p.result;
    return { :error("Empty model response") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("JSON parse fail") };
}

# ── Session helper ──
sub session-call(Str $op, %payload --> Hash) {
    my $sub = $nats.subscribe: my $inbox = "_INBOX.fs." ~ (^1_000_000).pick, :1max-messages;
    my $p   = $sub.supply.head.Promise;
    $nats.publish: "session.store.{$op}", to-json(%payload), :reply-to($inbox);
    await Promise.anyof: $p, Promise.in(10);
    $nats.unsubscribe: $sub;
    return { :error("Session store timeout") } unless $p.so;
    my $msg = $p.result;
    return { :error("Empty session response") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("Bad session JSON") };
}

# ── Save worker to disk and build Docker image ──
sub persist-worker(Str $name, Str $code) {
    my $dir  = "/workers/{$name}".IO;
    $dir.mkdir;

    # Write service.raku
    spurt($dir.add('service.raku'), $code);
    note "  💾 Saved {$dir}/service.raku";

    # Write Dockerfile (relative to build context)
    my $dockerfile = qq:to/END/;
FROM camelia-base:latest
COPY service.raku /app/service.raku
ENTRYPOINT ["raku", "/app/service.raku"]
END
    spurt($dir.add('Dockerfile'), $dockerfile);
    note "  💾 Saved {$dir}/Dockerfile";

    # Build Docker image
    my $image-name = "camelia-worker-{$name}:latest";
    note "  🏗️ Building {$image-name}...";
    my $proc = Proc::Async.new('docker', 'build', '-t', $image-name, $dir.absolute);
    my ($out, $err) = ('', '');
    $proc.stdout.lines(:chomp).tap(-> $l { $out ~= $l ~ "\n" });
    $proc.stderr.lines(:chomp).tap(-> $l { $err ~= $l ~ "\n" });
    my $result = await $proc.start;
    if $result.exitcode != 0 {
        note "  ❌ Build failed: {$err.substr(0, 300)}";
        return False;
    }
    note "  ✅ Image {$image-name} built";
    create-worker-stream($name);
    return True;

# ── Create JetStream stream for new worker type ──
sub create-worker-stream(Str $name) {
    my $stream-name = "WORKER_" ~ $name.uc.subst("-", "_");
    my $subject     = "worker.{$name}.task.>";

    note "  📡 Creating JetStream stream {$stream-name} ({$subject})...";

    my $config = to-json({
        :name($stream-name),
        :subjects([$subject]),
        :retention<limits>,
        :storage<file>,
        :max-age(86_400_000_000_000),
    });

    my $supply = $nats.request("\$JS.API.STREAM.CREATE.{$stream-name}", $config);
    my $msg = await $supply.head.Promise;
    if $msg && $msg.payload && !$msg.payload.starts-with("-ERR") {
        note "  ✅ Stream {$stream-name} created";
    } else {
        note "  ⚠️ Stream create: {$msg.?payload // "no response"} (may already exist)";
    }
}
}

# ── Generate code via model ──
sub generate-code(Str $prompt, %spec --> Str) {
    my $filled-template = $template;

    # Fill safe placeholders directly from spec
    $filled-template ~~ s:g/'{{NAME}}'/{ %spec<name> // '' }/;
    $filled-template ~~ s:g/'{{DESCRIPTION}}'/{ %spec<description> // '' }/;
    $filled-template ~~ s:g/'{{BASE_URL}}'/{ %spec<base_url> // '' }/;
    $filled-template ~~ s:g/'{{SUBJECT}}'/{ (%spec<subscriptions>[0]) // '' }/;

    # Model generates TOOLS_SCHEMA and TOOL_LOGIC
    my $gen-prompt = q:to/END/;
Fill in {{TOOLS_SCHEMA}} and {{TOOL_LOGIC}} in this template:

```
END
    $gen-prompt ~= $filled-template ~ "\n```\n\nSpecification: $prompt\n\nOnly output the COMPLETE template with both placeholders filled. No markdown fences, no explanations.";

    my @messages = (
        { :role<system>, :content($gen-system) },
        { :role<user>, :content($gen-prompt) },
    );

    my %resp = call-model(@messages);
    if %resp<error> {
        note "  ❌ Model error: {%resp<error>}";
        return '';
    }

    my $raw = %resp<choices>[0]<message><content> // '';
    # Strip markdown fences if present
    $raw ~~ s/^ '```' \w* \n?//;
    $raw ~~ s/\n? '```' $//;

    return $raw;
}

# ── Validate code ──
sub validate-code(Str $name, Str $code --> Hash) {
    my $tmpfile = "/tmp/factory-{$name}-{(^10000).pick}.raku";
    spurt($tmpfile, $code);
    END { unlink $tmpfile if $tmpfile.IO.e }

    my $proc = Proc::Async.new('raku', '-I/opt/nats.raku', '-c', $tmpfile);
    my $stdout = '';
    my $stderr = '';
    $proc.stdout.lines(:chomp).tap(-> $line { $stdout ~= $line ~ "\n" });
    $proc.stderr.lines(:chomp).tap(-> $line { $stderr ~= $line ~ "\n" });
    my $result = await $proc.start;

    unlink $tmpfile;

    if $result.exitcode == 0 && ($stdout ~~ /'Syntax OK'/ || $stderr ~~ /'Syntax OK'/) {
        return { :ok(True) };
    }

    return { :ok(False), :error($stderr || $stdout || "exit={$result.exitcode}") };
}

# ── Main handler ──
sub handle-factory-request(%req, Str $reply-to) {
    my $prompt = %req<prompt> // '';
    my %spec   = %req<spec>   // {};

    unless $prompt && %spec<name> {
        $nats.publish: $reply-to, to-json({
            :error("Missing 'prompt' or spec.name"),
        });
        return;
    }

    my $name = %spec<name>;
    note "🏭 Factory: building worker type '{$name}'...";

    # Generate → Validate loop (max 3 retries)
    my $code    = '';
    my $attempt = 0;
    my $success = False;
    my $error   = '';

    repeat {
        $attempt++;
        note "  🔄 Attempt {$attempt}/{$max-retries}...";

        $code = generate-code($prompt, %spec);
        unless $code {
            $error = "Model generation failed";
            last;
        }

        my %valid = validate-code($name, $code);
        if %valid<ok> {
            $success = True;
            note "  ✅ Syntax OK";
            last;
        }

        $error = %valid<error> // 'unknown validation error';
        note "  ❌ Validation failed: {$error.substr(0, 200)}";

        # Append error to prompt for retry
        %spec<previous_error> = $error;
        %spec<previous_code>  = $code.substr(0, 500);

    } while $attempt < $max-retries;

    unless $success {
        $nats.publish: $reply-to, to-json({
            :status<failed>,
            :$name,
            :attempts($attempt),
            :error($error),
        });
        note "  ❌ Factory: {$name} failed after {$attempt} attempts";
        return;
    }

    # Persist worker to disk and build Docker image
    my $built = persist-worker($name, $code);

    unless $built {
        $nats.publish: $reply-to, to-json({
            :status<code_generated>,
            :$name,
            :attempts($attempt),
            :error("Docker image build failed — code saved to /workers/{$name}/"),
        });
        note "  ⚠️ Factory: {$name} code generated but image build failed";
        return;
    }
    # Write to Worker Registry KV so orchestrator discovers this worker
    my $kv-subject = "\$KV.WORKER_REGISTRY.{$name}";
    $nats.publish: $kv-subject, to-json({
        :$name,
        :subject("worker.{$name}.task.>"),
        :description(%spec<description> // "Worker {$name}"),
        :topics([]),
    });
    note "  📋 Registered {$name} in KV_WORKER_REGISTRY";
    # Final response
    $nats.publish: $reply-to, to-json({
        :status<created>,
        :$name,
        :image("camelia-worker-{$name}:latest"),
        :attempts($attempt),
        :message("Worker {$name} created, validated, and Docker image built"),
    });

    note "  ✅ Factory: {$name} created in {$attempt} attempt(s)";
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

        my %req = try from-json($msg.payload);
        if $! {
            $nats.publish: $reply-to, to-json({ :error("Invalid JSON") });
            next;
        }

        $last-activity = now;
        lifecycle('busy');

        start {
            handle-factory-request(%req, $reply-to);

            $last-activity = now;
            lifecycle('idle');
        }
    }

    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service<worker-factory>,
                :$worker-id,
                :idle_seconds((now - $last-activity).Int),
            });
        }
    }
}
