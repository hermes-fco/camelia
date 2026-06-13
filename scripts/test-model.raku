#!/usr/bin/env raku
use Nats;
use JSON::Fast;

my $nats = Nats.new: :servers['nats://camelia-nats:4222'];
await $nats.start;
$nats.connect;
say "🟢 Connected";

my $inbox = "_INBOX.test-" ~ (^10000).pick;
my $sub = $nats.subscribe: $inbox, :max-messages(1);

my $body = to-json({
    model => 'deepseek-v4-pro',
    messages => [%( :role<user>, :content('Say "hello from camelia" and nothing else') )],
    temperature => 0.1,
});

say "📤 Publishing to model.deepseek.completion...";
$nats.publish: 'model.deepseek.completion', $body, :reply-to($inbox);

say "⏳ Waiting (60s)...";
my @msgs = await $sub.supply.head(1);
if @msgs && @msgs[0].payload {
    my %r = try from-json(@msgs[0].payload) // {};
    if %r<error> { say "❌ API error: {%r<error>}" }
    else {
        my $content = %r<choices>[0]<message><content> // 'NO CONTENT';
        say "✅ Model responded: {$content}";
    }
} else {
    say "❌ No response from model-deepseek";
}
