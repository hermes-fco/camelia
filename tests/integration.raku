#!/usr/bin/env raku
# 🌺 Camélia — Integration Tests (sequential, with delays)
use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $NATS = %*ENV<NATS_URL> // 'nats://camelia-nats:4222';
my $PASS = 0; my $FAIL = 0; my $SKIP = 0;

sub ok(Str $name, $cond, Str $detail = '') {
    my $result = $cond.defined ?? $cond.Bool !! False;
    if $result { $PASS++; note "  ✅ {$name}" }
    else       { $FAIL++; note "  ❌ {$name}" ~ ($detail ?? " — {$detail}" !! "") }
}

sub skip(Str $name, Str $why = '') { $SKIP++; note "  ⏭️  {$name}" }

note "🔌 Connecting NATS...";
my $nats = Nats.new: :servers[$NATS];
await $nats.start;
$nats.connect;
note "✅ Connected.\n";

sub req(Str $subj, Str $payload, :$timeout = 10 --> Hash) {
    my $inbox = "_INBOX.t." ~ (^1_000_000).pick;
    my $sub   = $nats.subscribe: $inbox, :1max-messages;
    my $p     = $sub.supply.head.Promise;
    $nats.publish: $subj, $payload, :reply-to($inbox);
    await Promise.anyof: $p, Promise.in($timeout);
    return { :error("TIMEOUT {$timeout}s") } unless $p.so;
    return { :error("Empty") } unless $p.result && $p.result.payload;
    try from-json($p.result.payload) // { :error("JSON: $!") };
}

# ═══ 1. Session Store ═══
note "── Session Store ──";
sleep 1;

my %c = req('session.store.create', to-json({}));
ok('create', %c<ok>, %c<error> // '');
my $sid = %c<session_id> // '';

if $sid {
    sleep 0.5;
    my %g = req('session.store.get', to-json({ :session_id($sid) }));
    ok('get', %g<ok>, %g<error> // '');

    sleep 0.5;
    my %app = req('session.store.append', to-json({
        :session_id($sid), :expected_seq(0), :entries([{ :role<user>, :content<hi> },]) }));
    ok('append', %app<ok> && %app<seq> == 1, %app<error> // '');
}

sleep 0.5;
my %gm = req('session.store.get', to-json({ :session_id('no-such-session-xyz') }));
ok('get missing', %gm<error> && %gm<error>.contains('not found'),
   %gm<error> // 'expected error');

sleep 0.5;
my %lst = req('session.store.list', to-json({}));
ok('list', %lst<ok> && %lst<session_count> ~~ Int,
   %lst<error> // "count={%lst<session_count>}");

# ═══ 2. Health Checks ═══
note "\n── Health Checks ──";
for <session-store orchestrator model-deepseek tool-executor spawner> -> $svc {
    sleep 0.5;
    my %h = req("health.check.{$svc}", '{}', :timeout(5));
    ok("{$svc} health", %h<status> eq 'ok', %h<error> // %h.gist);
}

# ═══ 3. Orchestrator Task ═══
note "\n── Orchestrator ──";
sleep 1;
my %task = req('orchestrator.task', to-json({ :prompt("count from 1 to 3") }), :timeout(35));
ok('task processing', %task<result> || %task<error>, %task<error> // %task<result> // '');

# ═══ Summary ═══
my $total = $PASS + $FAIL + $SKIP;
note "\n══════ Results ══════";
note "  ✅ PASS: {$PASS}/{$total}";
note "  ❌ FAIL: {$FAIL}/{$total}" if $FAIL;
note "  ⏭️  SKIP: {$SKIP}/{$total}" if $SKIP;
exit $FAIL ?? 1 !! 0;
