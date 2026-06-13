#!/usr/bin/env raku
# 🌺 Camelia PoC #2 — Agent with Tool Calling

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222';

# ── Tools schema (what the model can call) ──
my @tools = (
    {
        type     => "function",
        function => {
            name        => "run_shell",
            description => "Execute a shell command in the Linux sandbox and return stdout, stderr and exit code.",
            parameters  => {
                type       => "object",
                properties => {
                    command => { type => "string", description => "Shell command to execute" },
                },
                required => ["command"],
            },
        },
    },
    {
        type     => "function",
        function => {
            name        => "read_file",
            description => "Read a file from the sandbox and return its content with numbered lines.",
            parameters  => {
                type       => "object",
                properties => {
                    path   => { type => "string", description => "File path (relative to sandbox)" },
                    offset => { type => "integer", description => "Starting line (0-indexed, default 0)" },
                    limit  => { type => "integer", description => "Max lines (default 500)" },
                },
                required => ["path"],
            },
        },
    },
    {
        type     => "function",
        function => {
            name        => "write_file",
            description => "Write content to a file in the sandbox.",
            parameters  => {
                type       => "object",
                properties => {
                    path    => { type => "string", description => "File path (relative to sandbox)" },
                    content => { type => "string", description => "Content to write" },
                },
                required => ["path", "content"],
            },
        },
    },
);

# ── System prompt ──
my $system = q:to/END/;
You are a concise Linux terminal assistant. You can execute shell commands,
read and write files in the sandbox (/tmp/sandbox).
Be direct and practical — get straight to the point.
END

# ── User prompt ──
my $user-prompt = %*ENV<PROMPT> // 'List files in the current directory, then create a file called "hello.txt" with the text "Hello Camelia!" and show its contents.';

note "🟡 Connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 NATS connected.";

# ── Build initial message history ──
my @messages = (
    { :role<system>, :content($system) },
    { :role<user>,   :content($user-prompt) },
);

# ── Main conversation loop ──
my $max-turns = 10;
loop {
    last if $max-turns-- <= 0;

    my $request-id = (^2**32).pick.fmt('%08x');

    # Build request with tools
    my $request = to-json {
        :id($request-id),
        :model('deepseek-v4-pro'),
        :messages(@messages),
        :@tools,
        :tool_choice<auto>,
    };
    note "DEBUG payload: chars={$request.chars} bytes={$request.encode('utf8').bytes}";

    note "📤 Turn {5 - $max-turns}: sending to model (id=$request-id)...";

    # Inbox for model response
    my $model-inbox = "_INBOX.model." ~ (('a'..'z').pick xx 12).join;
    my $model-sub   = $nats.subscribe: $model-inbox;
    # Tap BEFORE publish — avoids race where reply arrives before listener
    my $model-reply = start await $model-sub.supply.head.Promise;

    $nats.publish: 'model.deepseek.completion', $request, :reply-to($model-inbox);

    note "DEBUG waiting for response on inbox $model-inbox...";
    my $model-msg = await $model-reply;
    $nats.unsubscribe: $model-sub;
    note "DEBUG response received! Defined={$model-msg.defined}, Payload={$model-msg.?payload.?chars // 'NONE'}";
    unless $model-msg && $model-msg.payload {
        note "❌ No response from model";
        last;
    }

    note "DEBUG agent payload: {$model-msg.payload.chars} chars, first 300: {$model-msg.payload.substr(0, 300)}";
    my %response = try from-json($model-msg.payload);
    if $! {
        note "❌ Invalid JSON from model: $!";
        last;
    }

    if %response<error> {
        note "❌ Model error: {%response<error>}";
        last;
    }

    my $choice  = %response<choices>[0];
    unless $choice {
        note "❌ No choices in response: {%response.keys}";
        last;
    }
    my $message = $choice<message> // {};
    my $finish  = $choice<finish_reason> // '';

    # If there's text content, show it
    if $message<content> {
        say "🤖 {$message<content>}";
    }

    # If it's a tool_call, process it
    if $finish eq 'tool_calls' || $message<tool_calls> {
        # Add assistant message to history
        @messages.push: $message;

        my @tool-calls = $message<tool_calls>.List;
        note "🔧 Model requested {+@tool-calls} tool call(s)";

        # Execute each tool call in parallel
        my @results;
        for @tool-calls -> $tc {
            my $fn     = $tc<function>;
            my $name   = $fn<name>;
            my $args   = try from-json($fn<arguments>) // {};
            my $tc-id  = $tc<id> // 'unknown';

            note "  ⚙️ {$name} (id={$tc-id})";

            # Publish to tool-executor with inbox
            my $tool-inbox = "_INBOX.tool." ~ (('a'..'z').pick xx 12).join;
            my $tool-sub   = $nats.subscribe: $tool-inbox;
            my $tool-reply = $tool-sub.supply.head.Promise;

            $nats.publish: "tools.exec.{$name}", to-json({
                :name($name),
                :tool_call_id($tc-id),
                :arguments($args),
            }), :reply-to($tool-inbox);

            my $tool-msg = await $tool-reply;
            $nats.unsubscribe: $tool-sub;
            if $tool-msg && $tool-msg.payload {
                my %result = try from-json($tool-msg.payload);
                @results.push: %result;
                note "  ✅ {$name} done";
            } else {
                @results.push: { :error("No response from tool executor") };
                note "  ❌ {$name} timeout";
            }
        }

        # Add tool results to history
        for @tool-calls Z @results -> ($tc, $result) {
            @messages.push: {
                :role<tool>,
                :tool_call_id($tc<id>),
                :content(to-json($result)),
            };
        }

        # Continue the loop — resend to model with results
        note "🔄 Resending to model with results...";
        next;
    }

    # finish_reason 'stop' — done
    if $finish eq 'stop' {
        # Add final message to history
        @messages.push: $message;
        note "✅ Conversation finished.";
        last;
    }

    # Other finish_reason (length, content_filter, etc)
    note "⚠️ finish_reason={$finish} — ending.";
    last;
}

$nats.stop;
