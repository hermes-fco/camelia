#!/usr/bin/env raku
use Nats;
use JSON::Fast;

my $nats = Nats.new: :servers['nats://camelia-nats:4222'];
await $nats.start;
$nats.connect;
say "🟢 Connected";

# Manual request: subscribe to temp inbox, then publish
my $inbox = "_INBOX.e2e-" ~ (^1000).pick;
my $sub = $nats.subscribe: $inbox, :max-messages(1);

my $task = to-json({ :prompt("What is 2 + 2? Answer with just the number."), :session_id("e2e-test") });

say "📤 Publishing to orchestrator.task...";
$nats.publish: 'orchestrator.task', $task, :reply-to($inbox);

say "⏳ Waiting for response (120s timeout)...";
my @msgs = await $sub.supply.head;
if @msgs && @msgs[0].payload {
    my %r = try from-json(@msgs[0].payload) // {};
    if %r<error> { say "❌ {%r<error>}" }
    else { say "✅ Result: {%r<result> // %r<message> // 'no content'}" }
} else {
    say "❌ No response";
}
