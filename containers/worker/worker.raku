#!/usr/bin/env raku
# 🌺 Camélia PoC #7 — Worker Agent (subscribe to worker.task.>)
#
# Listens for tasks, executes via tool-executor, responds with result.
# Self-terminates after 5 min idle.

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url  = %*ENV<NATS_URL>  // 'nats://127.0.0.1:4222';
my $worker-id = ('a'..'z').pick(6).join;

note "🟡 Worker {$worker-id} connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 Worker {$worker-id} connected.";

my $task-sub = $nats.subscribe: 'worker.task.>';
note "🟢 Listening on worker.task.>";

# Tool execution helper
sub exec-tool(Str $name, $tc-id, %args --> Hash) {
    my $sub   = $nats.subscribe: my $inbox = "_INBOX.tl." ~ (^1_000_000).pick, :1max-messages;
    my $p     = $sub.supply.head.Promise;
    $nats.publish: "tools.exec.{$name}", to-json({ :$name, :tool_call_id($tc-id), :arguments(%args) }), :reply-to($inbox);
    await Promise.anyof: $p, Promise.in(30);
    $nats.unsubscribe: $sub;
    return { :error("Tool timeout") } unless $p.so;
    my $msg = $p.result;
    return { :error("Empty tool response") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("Bad tool JSON") };
}

# Model call helper
my @tools = (
    { type => "function", function => {
        name => "run_shell", description => "Execute a shell command, return \{stdout, stderr, exit_code\}",
        parameters => { type => "object", properties => {
            command => { type => "string", description => "Shell command" } }, required => ["command"] } } },
);

sub call-model(@messages --> Hash) {
    my $sub = $nats.subscribe: my $inbox = "_INBOX.wkr." ~ (^1_000_000).pick, :1max-messages;
    my $p   = $sub.supply.head.Promise;
    $nats.publish: 'model.deepseek.completion', to-json({
        :model('deepseek-v4-pro'), :@messages, :@tools, :tool_choice<auto>,
    }), :reply-to($inbox);
    await Promise.anyof: $p, Promise.in(120);
    $nats.unsubscribe: $sub;
    return { :error("Model timeout") } unless $p.so;
    my $msg = $p.result;
    return { :error("Empty model response") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("JSON parse fail") };
}

my $system = q:to/END/;
You are a specialized worker agent. Complete the assigned task using available tools.
Be thorough — don't leave work half-done. Return a complete result.
END

react {
    whenever $task-sub.supply -> $msg {
        next unless $msg.payload;
        my $reply-to = $msg.?reply-to;
        unless $reply-to {
            note "⚠️ worker.task without reply-to, ignoring";
            next;
        }

        my %task = try from-json($msg.payload);
        if $! { $nats.publish: $reply-to, to-json({ :error("Invalid JSON") }); next; }

        my $task-text = %task<task> // '';
        my $role      = %task<role> // 'worker';
        my $task-id   = %task<id>   // 'unknown';

        note "📨 {$worker-id}: task {$task-id} — {$task-text.substr(0, 80)}...";

        my @messages = (
            { :role<system>, :content($system ~ "\nYour role: {$role}") },
            { :role<user>,   :content($task-text) },
        );

        my $final = '';
        my $turns = 5;
        loop {
            last if $turns-- <= 0;
            my %resp = call-model(@messages);
            if %resp<error> { $final = "ERROR: {%resp<error>}"; last; }

            my $choice = %resp<choices>[0];
            last unless $choice;
            my $message = $choice<message> // {};
            my $finish  = $choice<finish_reason> // '';

            if $message<content> { $final = $message<content> }

            if $finish eq 'tool_calls' || $message<tool_calls> {
                @messages.push: $message;
                for $message<tool_calls>.List -> $tc {
                    my $fn   = $tc<function>;
                    my $name = $fn<name>;
                    my %args = try from-json($fn<arguments>) // {};
                    note "  🔧 {$name}";
                    my %result = exec-tool($name, $tc<id>, %args);
                    @messages.push: { :role<tool>, :tool_call_id($tc<id>), :content(to-json(%result)) };
                }
                next;
            }

            if $finish eq 'stop' { @messages.push: $message; last }
            last;  # unknown finish reason
        }

        $nats.publish: $reply-to, to-json({ :$worker-id, :$role, :$task-id, :result($final) });
        note "✅ {$worker-id}: task {$task-id} done";
    }
}
