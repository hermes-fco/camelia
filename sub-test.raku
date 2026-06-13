use Nats;
$*ERR.out-buffer = False;
my $n = Nats.new(:servers[%*ENV<NATS_URL> // "nats://camelia-nats:4222"]);
await $n.start;
$n.connect;
note "SUBSCRIBED model.deepseek.completion";
my $sub = $n.subscribe("model.deepseek.completion", :max-messages(1));
note "SUB SID={$sub.sid}, waiting...";
my $msg = await $sub.supply.head.Promise;
if $msg && $msg.payload {
    note "GOT: {$msg.payload}";
    note "reply-to: {$msg.reply-to}" if $msg.?reply-to;
}
note "DONE";
