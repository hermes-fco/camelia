#!/usr/bin/env raku
use Nats;
use JSON::Fast;

my $nats = Nats.new: :servers['nats://camelia-nats:4222'];
await $nats.start;
$nats.connect;
say "🟢 Connected";

my $inbox = "_INBOX.e2e-" ~ (^1000).pick;
my $p = Promise.new;
my $sub = $nats.subscribe: $inbox;
$sub.supply.tap: -> $msg { try $p.keep($msg) };

my $task = to-json({ :prompt("What is 2 + 2? Answer with just the number.") });

say "📤 Publishing task (no session_id)...";
$nats.publish: 'orchestrator.task', $task, :reply-to($inbox);

say "⏳ Waiting (60s)...";
await Promise.anyof: $p, Promise.in(60);

if $p.so {
    my %r = try from-json($p.result.payload) // {};
    if %r<error> { say "❌ {%r<error>}" }
    else { say "✅ Result: {%r<result> // %r<message> // 'no content'}" }
} else {
    say "❌ Timeout";
}
