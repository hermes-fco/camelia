#!/usr/bin/env raku
use Nats;
use JSON::Fast;

my $nats = Nats.new: :servers['nats://camelia-nats:4222'];
await $nats.start;
$nats.connect;
say "🟢 Connected";

my $inbox = "_INBOX.e2e-" ~ (^1000).pick;
my $sub = $nats.subscribe: $inbox, :max-messages(1);

# No session_id — forces session-load to go straight to create
my $task = to-json({ :prompt("What is 2 + 2? Answer with just the number.") });

say "📤 Publishing task (no session_id)...";
$nats.publish: 'orchestrator.task', $task, :reply-to($inbox);

say "⏳ Waiting (120s)...";
my @msgs = await $sub.supply.head;
if @msgs && @msgs[0].payload {
    my %r = try from-json(@msgs[0].payload) // {};
    if %r<error> { say "❌ {%r<error>}" }
    else { say "✅ Result: {%r<result> // %r<message> // 'no content'}" }
} else {
    say "❌ No response";
}
