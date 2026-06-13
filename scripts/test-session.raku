#!/usr/bin/env raku
use Nats;
use JSON::Fast;

my $nats = Nats.new: :servers['nats://camelia-nats:4222'];
await $nats.start;
$nats.connect;
say "🟢 Connected";

# Test session-store directly
my $inbox = "_INBOX.ss-test-" ~ (^10000).pick;
my $sub = $nats.subscribe: $inbox, :max-messages(1);

say "📤 Testing session.store.create...";
$nats.publish: 'session.store.create', to-json({}), :reply-to($inbox);

my @msgs = await $sub.supply.head(1);
if @msgs && @msgs[0].payload {
    my %r = try from-json(@msgs[0].payload) // {};
    say "✅ Session store response:";
    for %r.kv { say "  {$_.key} = {$_.value.gist}" }
} else {
    say "❌ No response from session-store";
}
