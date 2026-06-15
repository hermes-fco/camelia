#!/usr/bin/env raku
# 🌺 Camélia Integration Tests — End-to-End Worker & Orchestrator Verification
#
# Tests that ALL workers are responsive, the orchestrator routes correctly,
# conversational path works, and web-browser can fetch/parse URLs.
#
# Usage: docker run --rm --network camelia_camelia -v /root/forks/nats.raku/lib:/tmp/nats-lib \
#          camelia-base:latest raku -I /tmp/nats-lib /tmp/tests/camelia-e2e.raku

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url = %*ENV<NATS_URL> // 'nats://nats:4222';

# ── Helpers ──
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;

sub request-reply(Str $subject, Str $payload, Int :$timeout = 30 --> Hash) {
    my $inbox = "_INBOX.test." ~ (^1_000_000).pick;
    my $sub   = $nats.subscribe: $inbox, :1max-messages;
    my $p     = $sub.supply.head.Promise;
    $nats.publish: $subject, $payload, :reply-to($inbox);
    await Promise.anyof: $p, Promise.in($timeout);
    $nats.unsubscribe: $sub;
    return { :error("No response") } unless $p.so;
    my $msg = $p.result;
    try from-json($msg.payload) // { :error("JSON parse fail") };
}

sub ok(Str $name, $condition, :$diag) {
    if $condition {
        say "✅ {$name}";
        return True;
    } else {
        say "❌ {$name}" ~ ($diag ?? " — {$diag}" !! "");
        return False;
    }
}

my $passed = 0;
my $failed = 0;

# ════════════════════════════════════════════
# TEST 1: All persistent workers respond to health checks
# ════════════════════════════════════════════
say "\n═══ Workers Health ═══";

my %workers = (
    shell        => 'health.check.worker.shell',
    system       => 'health.check.worker.system',
    factory      => 'health.check.worker.factory',
    web-browser  => 'health.check.web.browser',
    api-time     => 'health.check.api.time',
    raku-compile => 'health.check.raku.compile',
);

for %workers.kv -> $name, $subject {
    my %resp = request-reply($subject, '{}', :timeout(5));
    if ok("{$name} health", %resp<status> eq 'ok', :diag(%resp<error> // '')) {
        $passed++;
    } else {
        $failed++;
    }
}

# ════════════════════════════════════════════
# TEST 2: Web-browser can fetch a URL and extract text
# ════════════════════════════════════════════
say "\n═══ Web-browser Fetch ═══";

my %fetch = request-reply('worker.web-browser.task.webtest',
    to-json({ :task('https://httpbin.org/status/200') }),
    :timeout(15));

if ok("web-browser fetch", %fetch<ok> && (%fetch<text> // '') !~~ /not.found/, :diag(%fetch<error> // 'no text')) {
    $passed++;
} else {
    $failed++;
}

# ════════════════════════════════════════════
# TEST 3: Web-browser URL auto-detect
# ════════════════════════════════════════════
say "\n═══ Web-browser Auto-Detect ═══";

my %auto = request-reply('worker.web-browser.task.autotest',
    to-json({ :task('https://httpbin.org/html') }),
    :timeout(15));

my $has-content = (%auto<text> // '').chars > 100;
if ok("web-browser auto-detect URL", %auto<ok> && $has-content, :diag(%auto<error> // 'no content')) {
    $passed++;
} else {
    $failed++;
}

# ════════════════════════════════════════════
# TEST 4: Orchestrator conversational path (no workers)
# ════════════════════════════════════════════
say "\n═══ Orchestrator Conversational ═══";

my %conv = request-reply('orchestrator.task',
    to-json({ :prompt('Hello!'), :chat_id('test-conv') }),
    :timeout(30));

if ok("conversational response", %conv<result> && %conv<result>.chars > 0,
       :diag(%conv<error> // 'empty response')) {
    $passed++;
} else {
    $failed++;
}

# ════════════════════════════════════════════
# TEST 5: Session persistence
# ════════════════════════════════════════════
say "\n═══ Session Persistence ═══";

my %task1 = request-reply('orchestrator.task',
    to-json({ :prompt('My name is Fernando'), :chat_id('test-session') }),
    :timeout(30));

my $sid1 = %task1<session_id> // '';
if ok("task1 got session_id", $sid1.chars > 0) {
    $passed++;
} else {
    $failed++;
}

my %task2 = request-reply('orchestrator.task',
    to-json({ :prompt('What is my name?'), :session_id($sid1), :chat_id('test-session') }),
    :timeout(30));

my $result2 = %task2<result> // '';
if ok("task2 remembers name", $result2.contains('Fernando') || $result2.contains('fernando'),
       :diag("got: {$result2.substr(0, 100)}")) {
    $passed++;
} else {
    $failed++;
}

# ════════════════════════════════════════════
# TEST 6: System worker topics
# ════════════════════════════════════════════
say "\n═══ System Worker Topics ═══";

my %health = request-reply('worker.system.task.systest',
    to-json({ :topic<system_health> }),
    :timeout(10));

if ok("system_health", %health<ok> && %health<total> > 0,
       :diag(%health<error> // 'no total')) {
    $passed++;
} else {
    $failed++;
}

# ════════════════════════════════════════════
# TEST 7: Reconfigure topic exists
# ════════════════════════════════════════════
say "\n═══ Reconfigure Topic ═══";

my %list = request-reply('worker.system.task.listtest',
    to-json({ :topic<containers_list> }),
    :timeout(10));

if ok("containers_list", %list<ok> && (%list<count> // 0) > 0,
       :diag(%list<error> // 'empty list')) {
    $passed++;
} else {
    $failed++;
}

# ════════════════════════════════════════════
# SUMMARY
# ════════════════════════════════════════════
say "\n{'=' x 40}";
say "RESULTS: {$passed} passed, {$failed} failed";
say "{'=' x 40}";

exit $failed > 0 ?? 1 !! 0;
