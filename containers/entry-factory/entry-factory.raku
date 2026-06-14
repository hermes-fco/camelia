#!/usr/bin/env raku
# 🌺 Camélia — Entry Factory (meta-worker)
#
# Generates entry-point containers that bridge external protocols
# (Telegram, HTTP, WhatsApp, WebSocket) into the Camélia NATS mesh.
# Uses the model to fill protocol-specific listener logic.

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url      = %*ENV<NATS_URL>       // 'nats://127.0.0.1:4222';
my $model-subject = %*ENV<MODEL_SUBJECT>   // 'model.deepseek.completion';
my $max-retries   = %*ENV<FACTORY_RETRIES> // 3;

# ── Load template ──
my $template-path = '/opt/camelia/templates/entry-base.raku';
my $template      = $template-path.IO.e ?? $template-path.IO.slurp !! '';
die "Template not found at $template-path" unless $template;

# ── System prompt for entry generation ──
my $gen-system = q:to/END/;
You are a Raku code generator specialized in network entry points.
Given an entry-base template with {{PLACEHOLDERS}} and a specification,
fill in the template with working Raku code.

This is an ENTRY POINT — NOT a task worker. It does NOT handle tasks.
It LISTENS to external input (HTTP, Telegram, WebSocket, etc.) and
PUBLISHES messages into NATS. Optionally it SUBSCRIBES to a reply topic
to send responses back to the external source.

Template placeholders you must fill:
  {{LISTENER_LOGIC}} — The main listener: HTTP server (Cro::HTTP), Telegram
    long-poll loop, webhook handler, WebSocket server, etc. This code runs
    BEFORE the react block, setting up the external-facing listener.
    It should run asynchronously (start { ... } or Proc::Async).

  {{REACT_BLOCK}}   — whenever blocks inside the main react loop. This is
    where the entry subscribes to NATS reply topics and forwards responses
    back to the external source (e.g., send Telegram message, HTTP response).

  {{REPLY_LOGIC}}   — Helper subs for sending responses back to the external
    source (e.g., send-telegram-message, http-respond). Called from within
    the react block's whenever clauses.

Placeholders already filled by the factory (do NOT change):
  {{NAME}}, {{DESCRIPTION}}, {{PROTOCOL}}, {{SUBJECT}}, {{PORT}}

Rules:
- Output ONLY the filled template, nothing else — no markdown fences
- Use Proc::Async for subprocess calls, never shell()
- HTTP servers: use Cro::HTTP if available, otherwise a simple IO::Socket::INET loop
- Telegram bots: use long-polling with HTTP::Tiny (GET /bot<token>/getUpdates)
- ALL external data must be published to NATS via the emit() helper
- emit() is already defined in the template — USE IT, don't redefine
- Secrets come from %*ENV — never hardcode tokens or keys
- Handle connection errors gracefully with try/CATCH
- Keep the react loop responsive — use start {} for long operations

Entry types and their patterns:
- Telegram: loop { GET updates; for each message → emit('entry.telegram.message'); sleep 1 }
  Reply: subscribe 'entry.telegram.response' → POST sendMessage to Telegram API
- Webhook (HTTP): listen on port; POST / → emit('entry.http.raw'); return 200
  Reply: subscribe 'entry.http.response' → forward to callback URL
- WebSocket: accept connection; for each frame → emit('entry.ws.event')
  Reply: subscribe 'entry.ws.send' → send frame to socket
- WhatsApp: webhook receiver (POST /webhook → emit('entry.whatsapp.message'))
  Reply: subscribe 'entry.whatsapp.response' → POST to WhatsApp Business API
END

# ── Connect NATS ──
note "🟡 Entry Factory connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 Entry Factory connected.";

my $task-sub   = $nats.subscribe: 'entry.factory.request';
my $health-sub = $nats.subscribe: 'health.check.entry.factory';
note "🟢 Listening on entry.factory.request";

# ── Model call helper ──
sub call-model(@messages, :$temperature = 0.1 --> Hash) {
    my %body = :model('deepseek-v4-pro'), :@messages, :$temperature;
    my $sub = $nats.subscribe: my $inbox = "_INBOX.ef." ~ (^1_000_000).pick, :1max-messages;
    my $p   = $sub.supply.head.Promise;
    $nats.publish: $model-subject, to-json(%body), :reply-to($inbox);
    await Promise.anyof: $p, Promise.in(120);
    $nats.unsubscribe: $sub;
    return { :error("Model timeout") } unless $p.so;
    my $msg = $p.result;
    return { :error("Empty model response") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("JSON parse fail") };
}

# ── Persist entry to disk ──
sub persist-entry(Str $name, Str $code, Str $protocol, Int $port) {
    my $dir = "/entries/{$name}".IO;
    $dir.mkdir;

    spurt($dir.add('service.raku'), $code);
    note "  💾 Saved {$dir}/service.raku";

    my $dockerfile = qq:to/END/;
FROM camelia-base:latest
COPY entries/{$name}/service.raku /app/service.raku
COPY templates/ /opt/camelia/templates/
ENV ENTRY_NAME={$name}
ENV ENTRY_PORT={$port}
EXPOSE {$port}
ENTRYPOINT ["raku", "/app/service.raku"]
END
    spurt($dir.add('Dockerfile'), $dockerfile);
    note "  💾 Saved {$dir}/Dockerfile";

    return True;
}

# ── Generate code via model ──
sub generate-code(Str $prompt, %spec --> Str) {
    my $filled = $template;

    # Fill safe placeholders
    $filled ~~ s:g/'{{NAME}}'/{ %spec<name> // '' }/;
    $filled ~~ s:g/'{{DESCRIPTION}}'/{ %spec<description> // '' }/;
    $filled ~~ s:g/'{{PROTOCOL}}'/{ %spec<protocol> // '' }/;
    $filled ~~ s:g/'{{PORT}}'/{ %spec<port> // '8080' }/;

    my $gen-prompt = q:to/END/;
Fill in {{LISTENER_LOGIC}}, {{REPLY_LOGIC}}, and {{REACT_BLOCK}} in this entry template:

```
END
    $gen-prompt ~= $filled ~ "\n```\n\nSpecification: $prompt\n\nOnly output the COMPLETE template with all three placeholders filled. No markdown fences, no explanations.";

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
    $raw ~~ s/^ '```' \w* \n?//;
    $raw ~~ s/\n? '```' $//;

    return $raw;
}

# ── Validate code ──
sub validate-code(Str $name, Str $code --> Hash) {
    my $tmpfile = "/tmp/entry-factory-{$name}-{(^10000).pick}.raku";
    spurt($tmpfile, $code);
    END { unlink $tmpfile if $tmpfile.IO.e }

    my $proc = Proc::Async.new('raku', '-I/opt/nats.raku', '-c', $tmpfile);
    my $stdout = '';
    my $stderr = '';
    $proc.stdout.lines(:chomp).tap(-> $l { $stdout ~= $l ~ "\n" });
    $proc.stderr.lines(:chomp).tap(-> $l { $stderr ~= $l ~ "\n" });
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

    unless $prompt && %spec<name> && %spec<protocol> {
        $nats.publish: $reply-to, to-json({
            :error("Missing 'prompt', spec.name, or spec.protocol"),
        });
        return;
    }

    my $name     = %spec<name>;
    my $protocol = %spec<protocol>;
    my $port     = %spec<port> // 8080;
    note "🏭 Entry Factory: building entry '{$name}' ({$protocol})...";

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

        %spec<previous_error> = $error;
        %spec<previous_code>  = $code.substr(0, 500);

    } while $attempt < $max-retries;

    unless $success {
        $nats.publish: $reply-to, to-json({
            :status<failed>,
            :$name,
            :$protocol,
            :attempts($attempt),
            :error($error),
        });
        note "  ❌ Entry Factory: {$name} failed after {$attempt} attempts";
        return;
    }

    persist-entry($name, $code, $protocol, $port);

    $nats.publish: $reply-to, to-json({
        :status<created>,
        :$name,
        :$protocol,
        :$port,
        :attempts($attempt),
        :message("Entry {$name} created and validated"),
    });

    note "  ✅ Entry Factory: {$name} created in {$attempt} attempt(s)";
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

        my %req = try from-json($msg.payload);
        if $! {
            $nats.publish: $reply-to, to-json({ :error("Invalid JSON") });
            next;
        }

        start {
            handle-factory-request(%req, $reply-to);
        }
    }

    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service<entry-factory>,
            });
        }
    }
}
