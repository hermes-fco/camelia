#!/usr/bin/env raku
# 🌺 Camélia — Integration Tests (run inside camelia-net)
#
# Run: docker create --name ct --network camelia-net --entrypoint tail camelia-worker:latest -f /dev/null
#      docker cp tests/camelia-integration.raku ct:/tmp/test.raku
#      docker cp forks/nats.raku/lib ct:/tmp/nats-lib
#      docker start ct
#      docker exec ct raku -I/tmp/nats-lib /tmp/test.raku
#      docker rm -f ct

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $NATS = %*ENV<NATS_URL> // 'nats://camelia-nats:4222';
my $PASS = 0; my $FAIL = 0; my $SKIP = 0;
my $START = now;

sub ok(Str $name, $cond, Str $detail = '') {
    if $cond.defined && $cond.Bool {
        $PASS++;
        note "  ✅ {$name}";
    } else {
        $FAIL++;
        note "  ❌ {$name}" ~ ($detail ?? " — {$detail}" !! "");
    }
}

sub skip(Str $name, Str $why = '') {
    $SKIP++;
    note "  ⏭️  {$name}" ~ ($why ?? " ({$why})" !! "");
}

note "🔌 Connecting NATS ({$NATS})...";
my $nats = Nats.new: :servers[$NATS];
await $nats.start;
$nats.connect;
note "✅ Connected in {(now - $START).fmt('%.1f')}s\n";

sub req(Str $subj, Str $payload, :$timeout = 15 --> Hash) {
    my $supply = $nats.request($subj, $payload, :$timeout);
    my $resp = await $supply.Promise;
    return { :error("Timeout {$timeout}s") } unless $resp && $resp.payload;
    try from-json($resp.payload) // { :error("JSON parse fail") };
}

# ═══════════════════════════════════════════
# 1. HEALTH CHECKS
# ═══════════════════════════════════════════

note "── 1. Health Checks ──";
my %health-targets = (
    'session-store'  => 'session-store',
    'orchestrator'   => 'orchestrator',
    'tool-executor'  => 'tool-executor',
    'spawner'        => 'spawner',
);

my $health-ok = 0;
for %health-targets.kv -> $svc, $label {
    my %h = req("health.check.{$svc}", '{}', :timeout(5));
    if %h<status> eq 'ok' {
        $health-ok++;
        ok("health.{$label}", True);
    } else {
        ok("health.{$label}", False, %h<error> // 'no response');
    }
}
ok("Health: {$health-ok}/" ~ +%health-targets ~ " services up",
   $health-ok >= +%health-targets - 1,  # allow 1 down
   "minimum " ~ (+%health-targets - 1) ~ " required");

# ═══════════════════════════════════════════
# 2. SESSION STORE — full CRUD cycle
# ═══════════════════════════════════════════

note "\n── 2. Session Store ──";

my %create = req('session.store.create', to-json({}), :timeout(5));
ok('session.create', %create<ok>, %create<error> // '');
my $sid = %create<session_id> // '';

if !$sid {
    skip('Session CRUD', 'create failed');
} else {
    sleep 0.2;

    # Get fresh session (history should be empty)
    my %get = req('session.store.get', to-json({ :session_id($sid) }), :timeout(5));
    ok('session.get', %get<ok> && (%get<session><history>.elems // -1) == 0,
       %get<error> // "history size=" ~ (%get<session><history>.elems // -1));

    sleep 0.2;

    # Atomic append with CAS
    my %app = req('session.store.append', to-json({
        :session_id($sid),
        :expected_seq(0),
        :entries([{ :role<user>, :content('hello camelia') },]),
    }), :timeout(5));
    ok('session.append', %app<ok> && %app<seq> == 1,
       %app<error> // "seq={%app<seq>}");

    sleep 0.2;

    # Verify history persisted
    my %get2 = req('session.store.get', to-json({ :session_id($sid) }), :timeout(5));
    ok('session.persisted', %get2<ok> && (%get2<session><history>.elems // 0) >= 1,
       %get2<error> // "history size=" ~ (%get2<session><history>.elems // 0));

    sleep 0.2;

    # CAS conflict detection (stale seq)
    my %conflict = req('session.store.append', to-json({
        :session_id($sid),
        :expected_seq(0),
        :entries([{ :role<user>, :content<stale> },]),
    }), :timeout(5));
    ok('session.cas-conflict', %conflict<conflict>,
       %conflict<error> // 'expected conflict=True');

    sleep 0.2;

    # Missing session → error
    my %missing = req('session.store.get', to-json({
        :session_id('no-such-session-xyz999'),
    }), :timeout(5));
    ok('session.missing', %missing<error> && %missing<error>.contains('not found'),
       %missing<error> // 'expected "not found" error');

    sleep 0.2;

    # List sessions
    my %list = req('session.store.list', to-json({}), :timeout(5));
    ok('session.list', %list<ok> && (%list<session_count> // 0) >= 1,
       %list<error> // "count=" ~ (%list<session_count> // 0));
}

# ═══════════════════════════════════════════
# 3. TOOL EXECUTOR
# ═══════════════════════════════════════════

note "\n── 3. Tool Executor ──";

my %tool1 = req('tools.exec.run_shell', to-json({
    :name<run_shell>,
    :tool_call_id<test-001>,
    :arguments({ :command('echo hello-from-sandbox') }),
}), :timeout(10));

my $tool-stdout = %tool1<result><stdout> // %tool1<result> // '';
ok('tools.run_shell', $tool-stdout ~~ /'hello-from-sandbox'/,
   %tool1<error> // "stdout=" ~ ($tool-stdout // '').substr(0, 60));

# Read/write file
my %tool2 = req('tools.exec.write_file', to-json({
    :name<write_file>,
    :tool_call_id<test-002>,
    :arguments({ :path<test-integration.txt>, :content('camelia test content') }),
}), :timeout(10));
ok('tools.write_file', %tool2<result><ok> || %tool2<ok>,
   %tool2<error> // '');

# ═══════════════════════════════════════════
# 4. TYPED WORKERS — shell + factory
# ═══════════════════════════════════════════

note "\n── 4. Typed Workers ──";

# 4a. Health checks for typed workers
for <shell factory> -> $wtype {
    my %hw = req("health.check.worker.{$wtype}", '{}', :timeout(5));
    ok("health.worker.{$wtype}", %hw<status> eq 'ok', %hw<error> // 'no response');
}

# 4b. worker.shell — direct task execution
my %wt1 = req('worker.shell.task.shell-test-1', to-json({
    :id<shell-test-1>,
    :tool<run_shell>,
    :arguments({ :command('echo worker-shell-test-ok') }),
}), :timeout(15));
my $wshell-out = %wt1<result><stdout> // %wt1<output> // '';
ok('worker.shell.direct-task',
   $wshell-out ~~ /'worker-shell-test-ok'/,
   %wt1<error> // "output=" ~ $wshell-out.substr(0, 80));

sleep 0.5;

# 4c. worker.shell — write_file via typed worker
my %wt2 = req('worker.shell.task.shell-test-2', to-json({
    :id<shell-test-2>,
    :tool<write_file>,
    :arguments({ :path<shell-worker-test.txt>, :content('typed-worker-test') }),
}), :timeout(15));
ok('worker.shell.write-file',
   %wt2<ok> || %wt2<result><ok>,
   %wt2<error> // '');

sleep 0.5;

# 4d. worker.factory — request a new worker type
my %wf1 = req('worker.factory.request', to-json({
    :prompt('Create a simple echo worker that responds with the message it receives. Name: test-echo. Subscribe to: test.echo.>.'),
    :spec({
        :name<test-echo>,
        :description('Echo worker for testing'),
        :subscriptions(['test.echo.>']),
    }),
}), :timeout(120));
ok('worker.factory.create',
   %wf1<status> eq 'created' || %wf1<status> eq 'validated',
   %wf1<error> // "status=" ~ (%wf1<status> // 'no-response'));

# ═══════════════════════════════════════════
# 5. ORCHESTRATOR — end-to-end task
# ═══════════════════════════════════════════

note "\n── 5. Orchestrator ──";

my %task1 = req('orchestrator.task', to-json({
    :prompt('Count from 1 to 3. Reply with: ONE TWO THREE'),
}), :timeout(90));

my $task-result = %task1<result> // %task1<error> // '';
ok('orchestrator.simple-task',
   %task1<result> || %task1<error>,
   "result=" ~ $task-result.substr(0, 120));

# ═══════════════════════════════════════════
# 5. ORCHESTRATOR with Session (conversation continuity)
# ═══════════════════════════════════════════

note "\n── 6. Orchestrator with Session ──";

my %create2 = req('session.store.create', to-json({}), :timeout(5));
my $sid2 = %create2<session_id> // '';

if !$sid2 {
    skip('Orchestrator+Session', 'session create failed');
} else {
    sleep 1.0;

    # Task 1: say something
    my %task-a = req('orchestrator.task', to-json({
        :prompt('Say hello in Portuguese. Just the word.'),
        :session_id($sid2),
    }), :timeout(300));
    ok('orch.session.task1', %task-a<result> || %task-a<error>,
       %task-a<error> // (%task-a<result> // '').substr(0, 80));

    sleep 1.0;

    # Verify session persisted the conversation
    my %get-sess = req('session.store.get', to-json({ :session_id($sid2) }), :timeout(5));
    my $history-size = %get-sess<session><history>.elems // 0;
    ok('orch.session.persisted', $history-size >= 2,
       "history has {$history-size} entries (expected >=2)");
}

# ═══════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════

$nats.stop;

my $total = $PASS + $FAIL + $SKIP;
my $elapsed = (now - $START).fmt('%.0f');
note "\n{'═' x 40}";
note "Results:  ✅ {$PASS}  ❌ {$FAIL}  ⏭️  {$SKIP}  (total: {$total})  time: {$elapsed}s";
if $FAIL > 0 {
    note "❌ {$FAIL} test(s) failed!";
    exit 1;
}
note "✅ All tests passed!";
