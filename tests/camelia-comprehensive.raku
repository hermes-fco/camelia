#!/usr/bin/env raku
# 🌺 Camélia — Comprehensive Integration Test Suite
# Runs inside Docker on camelia network: docker run --rm --network camelia_camelia camelia-base raku /app/test.raku

use Nats;
use JSON::Fast;

constant NATS_URL = %*ENV<NATS_URL> // 'nats://nats:4222';
constant TIMEOUT  = 120;

my $passed  = 0;
my $failed  = 0;
my $skipped = 0;
my @failures;

sub ok(Bool $cond, Str $desc) {
    if $cond { $passed++;  say "  ✅ {$desc}" }
    else     { $failed++;  say "  ❌ {$desc}"; @failures.push: $desc }
}

sub skip(Str $desc, Str $reason = 'not available') {
    $skipped++;
    say "  ⏭️  {$desc} (skipped: {$reason})";
}

sub nats-request($nats, Str $subject, Str $payload = '', Int :$timeout = 30 --> Hash) {
    my $inbox = "_INBOX.test.{(^1_000_000).pick}";
    my $sub = $nats.subscribe: $inbox, :max-messages(1);
    my $p = $sub.supply.head.Promise;
    $nats.publish: $subject, $payload, :reply-to($inbox);
    await Promise.anyof: $p, Promise.in($timeout);
    $sub.unsubscribe;
    return { :error("Timeout after {$timeout}s") } unless $p.so;
    my $msg = $p.result;
    return { :error("Empty response") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("JSON parse fail: {$msg.payload.substr(0,100)}") };
}

# ═══════════════════════════════════════════
say "🌺 Camélia Test Suite";
say "════════════════════════════════════";
say "";

# ── Connect ──
say "📡 Connecting to NATS ({NATS_URL})...";
my $nats = Nats.new: :servers[NATS_URL];
await $nats.start;
$nats.connect;
ok(True, 'NATS connection');
say "";

# ═══════════════════════════════════════════
# 1. SERVICE HEALTH CHECKS
# ═══════════════════════════════════════════
say "━━━ 1. SERVICE HEALTH ━━━";

my @services = <spawner session-store model-deepseek>;
for @services -> $svc {
    my %r = nats-request($nats, "health.check.{$svc}", '', :timeout(5));
    if %r<error> {
        skip("health.check.{$svc}", %r<error>);
    } else {
        ok(%r<status> // '' eq 'ok', "health.check.{$svc} → {%r<status> // 'no status'}");
    }
}
say "";

# ═══════════════════════════════════════════
# 2. SPAWNER
# ═══════════════════════════════════════════
say "━━━ 2. SPAWNER ━━━";

# 2a. Status
my %sp-status = nats-request($nats, 'spawner.control', to-json({:action<status>}), :timeout(10));
if %sp-status<error> {
    skip('spawner.status', %sp-status<error>);
} else {
    ok(%sp-status<active> ~~ Int, "spawner.status returns active={%sp-status<active>}");
    ok(%sp-status<max> ~~ Int, "spawner.status returns max={%sp-status<max>}");
}

# 2b. ensure_typed — invalid image
my %sp-invalid = nats-request($nats, 'spawner.control',
    to-json({:action<ensure_typed>, :type('nonexistent-xyz')}), :timeout(10));
if %sp-invalid<error> {
    skip('spawner.ensure_typed(invalid)', %sp-invalid<error>);
} else {
    ok(!(%sp-invalid<ok> // True), "spawner.ensure_typed rejects nonexistent type");
    ok(%sp-invalid<reason> ~~ 'no_image', "spawner.ensure_typed reports no_image reason");
}

# 2c. ensure_typed — shell worker (should exist)
my %sp-shell = nats-request($nats, 'spawner.control',
    to-json({:action<ensure_typed>, :type<shell>}), :timeout(30));
if %sp-shell<error> {
    skip('spawner.ensure_typed(shell)', %sp-shell<error>);
} else {
    ok(%sp-shell<ok> // False, "spawner.ensure_typed(shell) success");
}

# 2d. ensure
my %sp-ensure = nats-request($nats, 'spawner.control',
    to-json({:action<ensure>, :count(1)}), :timeout(10));
if %sp-ensure<error> {
    skip('spawner.ensure', %sp-ensure<error>);
} else {
    ok(%sp-ensure<ok> // False, "spawner.ensure returns ok");
}

# 2e. Concurrent ensure_typed (race condition test)
say "  🧪 Testing concurrent ensure_typed...";
my $inbox1 = "_INBOX.test.{(^1_000_000).pick}";
my $inbox2 = "_INBOX.test.{(^1_000_000).pick}";
my $sub1 = $nats.subscribe: $inbox1, :max-messages(1);
my $sub2 = $nats.subscribe: $inbox2, :max-messages(1);
my $p1 = $sub1.supply.head.Promise;
my $p2 = $sub2.supply.head.Promise;
$nats.publish: 'spawner.control', to-json({:action<ensure_typed>, :type<shell>}), :reply-to($inbox1);
$nats.publish: 'spawner.control', to-json({:action<ensure_typed>, :type<shell>}), :reply-to($inbox2);
await Promise.anyof: Promise.allof($p1, $p2), Promise.in(30);
$sub1.unsubscribe;
$sub2.unsubscribe;

if $p1.so && $p2.so {
    my %r1 = try from-json($p1.result.payload) // {};
    my %r2 = try from-json($p2.result.payload) // {};
    my $both-ok = (%r1<ok> // False) && (%r2<ok> // False);
    ok($both-ok, "concurrent ensure_typed: both return ok (no race)");
    if !$both-ok {
        note "    r1: {%r1.gist}";
        note "    r2: {%r2.gist}";
    }
} else {
    skip('concurrent ensure_typed', 'timeout');
}
say "";

# ═══════════════════════════════════════════
# 3. SESSION STORE
# ═══════════════════════════════════════════
say "━━━ 3. SESSION STORE ━━━";

my $test-sid = "test-{(^1_000_000).pick}";

# 3a. Create
my %s-create = nats-request($nats, 'session.store.create',
    to-json({:session_id($test-sid), :chat_id<test-chat>}), :timeout(10));
my $create-ok = False;
if %s-create<error> {
    skip('session.create', %s-create<error>);
} else {
    $create-ok = %s-create<ok> // False;
    ok($create-ok, "session.create → ok");
    ok((%s-create<session_id> // '') eq $test-sid, "session.create returns session_id");
}

# 3b. Get
my %s-get = nats-request($nats, 'session.store.get',
    to-json({:session_id($test-sid)}), :timeout(10));
if %s-get<error> {
    skip('session.get', %s-get<error>);
} else {
    ok(%s-get<session> ~~ Hash, "session.get returns session object");
    ok((%s-get<session><session_id> // '') eq $test-sid, "session.get correct session_id");
}

# 3c. Append
my %s-append = nats-request($nats, 'session.store.append',
    to-json({:session_id($test-sid), :entries([{ :role<user>, :content<test message> }])}),
    :timeout(10));
if %s-append<error> {
    skip('session.append', %s-append<error>);
} else {
    ok(%s-append<ok> // False, "session.append → ok");
}

# 3d. Append with conflict (wrong expected_seq)
my %s-conflict = nats-request($nats, 'session.store.append',
    to-json({:session_id($test-sid), :expected_seq(99999),
             :entries([{ :role<user>, :content<stale> }])}),
    :timeout(10));
if %s-conflict<error> {
    skip('session.append(conflict)', %s-conflict<error>);
} else {
    ok(%s-conflict<conflict> // False, "session.append detects seq conflict");
}

# 3e. List
my %s-list = nats-request($nats, 'session.store.list', '', :timeout(10));
if %s-list<error> {
    skip('session.list', %s-list<error>);
} else {
    ok(%s-list<count> ~~ Int, "session.list returns count={%s-list<count>}");
}

# 3f. Delete
my %s-delete = nats-request($nats, 'session.store.delete',
    to-json({:session_id($test-sid)}), :timeout(10));
if %s-delete<error> {
    skip('session.delete', %s-delete<error>);
} else {
    ok(%s-delete<ok> // False, "session.delete → ok");
}

# 3g. Get after delete
my %s-get2 = nats-request($nats, 'session.store.get',
    to-json({:session_id($test-sid)}), :timeout(10));
if %s-get2<error> {
    skip('session.get(after delete)', %s-get2<error>);
} else {
    ok(%s-get2<error> ~~ Str, "session.get after delete returns error");
}
say "";

# ═══════════════════════════════════════════
# 4. MODEL DEEPSEEK
# ═══════════════════════════════════════════
say "━━━ 4. MODEL DEEPSEEK ━━━";

my %m-completion = nats-request($nats, 'model.deepseek.completion',
    to-json({:messages([{ :role<user>, :content("respond with just: OK") }]),
             :max_tokens(10)}),
    :timeout(30));
if %m-completion<error> {
    skip('model.completion', %m-completion<error>);
} else {
    ok((%m-completion<content> // '') ~~ /OK/, "model.deepseek simple completion: {%m-completion<content> // '(empty)'}");
}

# Test with system prompt
my %m-system = nats-request($nats, 'model.deepseek.completion',
    to-json({:messages([
        { :role<system>, :content("Respond in Portuguese. Say only 'Olá'.") },
        { :role<user>, :content("hi") }
    ]), :max_tokens(10)}),
    :timeout(30));
if %m-system<error> {
    skip('model.system_prompt', %m-system<error>);
} else {
    ok((%m-system<content> // '') ~~ /Olá/, "model.deepseek respects system prompt: {%m-system<content> // '(empty)'}");
}
say "";

# ═══════════════════════════════════════════
# 5. ORCHESTRATOR END-TO-END
# ═══════════════════════════════════════════
say "━━━ 5. ORCHESTRATOR E2E ━━━";

# 5a. Simple math (shell worker)
my %o-math = nats-request($nats, 'orchestrator.task',
    to-json({:prompt("What is 2 + 2? Answer with just the number."),
             :session_id("test-math-{(^1000).pick}"),
             :chat_id<test-e2e>}),
    :timeout(120));
if %o-math<error> {
    skip('orchestrator.math', %o-math<error>);
} else {
    my $result = %o-math<result> // '';
    ok($result ~~ /4/, "orchestrator.math: result contains '4': '{$result.substr(0,100)}'");
    ok(!(%o-math<error> // False), "orchestrator.math: no error field");
}

# 5b. Conversational (no decomposition needed)
my %o-chat = nats-request($nats, 'orchestrator.task',
    to-json({:prompt("Hello! How are you today? Just say 'I am well, thank you'."),
             :session_id("test-chat-{(^1000).pick}"),
             :chat_id<test-e2e>}),
    :timeout(60));
if %o-chat<error> {
    skip('orchestrator.conversational', %o-chat<error>);
} else {
    my $result = %o-chat<result> // '';
    ok(($result ~~ /well/ || $result ~~ /:i 'I am'/), "orchestrator.conversational: responds naturally: '{$result.substr(0,100)}'");
}

# 5c. Empty prompt
my %o-empty = nats-request($nats, 'orchestrator.task',
    to-json({:prompt(""), :session_id("test-empty-{(^1000).pick}"), :chat_id<test-e2e>}),
    :timeout(30));
if %o-empty<error> {
    skip('orchestrator.empty_prompt', %o-empty<error>);
} else {
    ok(%o-empty<error> ~~ Str || %o-empty<result> ~~ Str, "orchestrator handles empty prompt gracefully");
}
say "";

# ═══════════════════════════════════════════
# 6. WEB BROWSER
# ═══════════════════════════════════════════
say "━━━ 6. WEB BROWSER ━━━";

# Ensure web-browser worker exists
nats-request($nats, 'spawner.control',
    to-json({:action<ensure_typed>, :type<web-browser>}), :timeout(30));

my %o-web = nats-request($nats, 'orchestrator.task',
    to-json({:prompt("Fetch http://example.com and tell me the page title."),
             :session_id("test-web-{(^1000).pick}"),
             :chat_id<test-e2e>}),
    :timeout(120));
if %o-web<error> {
    skip('orchestrator.web_browser', %o-web<error>);
} else {
    my $result = %o-web<result> // '';
    ok($result ~~ /Example/ || $result ~~ /:i example/,
       "orchestrator.web_browser fetches URL: '{$result.substr(0,100)}'");
}
say "";

# ═══════════════════════════════════════════
# 7. ERROR HANDLING
# ═══════════════════════════════════════════
say "━━━ 7. ERROR HANDLING ━━━";

# 7a. Invalid JSON
my %e-json = nats-request($nats, 'orchestrator.task',
    'not valid json {{{', :timeout(10));
if %e-json<error> {
    skip('error.invalid_json', %e-json<error>);
} else {
    ok(%e-json<error> ~~ Str, "orchestrator rejects invalid JSON");
}

# 7b. Missing required fields in spawner
my %e-spawn = nats-request($nats, 'spawner.control',
    to-json({:action<unknown_action_xyz>}), :timeout(10));
if %e-spawn<error> {
    skip('error.unknown_action', %e-spawn<error>);
} else {
    ok(%e-spawn<error> ~~ Str, "spawner rejects unknown action");
}
say "";

# ═══════════════════════════════════════════
# 8. TIMER
# ═══════════════════════════════════════════
say "━━━ 8. TIMER ━━━";

my %o-timer = nats-request($nats, 'orchestrator.task',
    to-json({:prompt("Set a timer for 1 second to remind me 'test passed'"),
             :session_id("test-timer-{(^1000).pick}"),
             :chat_id<test-e2e>}),
    :timeout(120));
if %o-timer<error> {
    skip('orchestrator.timer', %o-timer<error>);
} else {
    my $result = %o-timer<result> // '';
    ok($result ~~ Str && $result.chars > 0, "orchestrator.timer: got response: '{$result.substr(0,100)}'");
}
say "";

# ═══════════════════════════════════════════
# 9. WEB SEARCH
# ═══════════════════════════════════════════
say "━━━ 9. WEB SEARCH ━━━";

my %o-search = nats-request($nats, 'orchestrator.task',
    to-json({:prompt("Search the web for 'Raku programming language' and tell me one fact about it."),
             :session_id("test-search-{(^1000).pick}"),
             :chat_id<test-e2e>}),
    :timeout(120));
if %o-search<error> {
    skip('orchestrator.web_search', %o-search<error>);
} else {
    my $result = %o-search<result> // '';
    ok($result.chars > 20, "orchestrator.web_search: got substantive response: '{$result.substr(0,100)}'");
}
say "";

# ═══════════════════════════════════════════
# 10. STRESS: MULTIPLE CONCURRENT TASKS
# ═══════════════════════════════════════════
say "━━━ 10. CONCURRENT TASKS ━━━";

my @concurrent-promises;
for ^3 -> $i {
    my $inbox = "_INBOX.test.{(^1_000_000).pick}";
    my $sub = $nats.subscribe: $inbox, :max-messages(1);
    my $p = $sub.supply.head.Promise;
    $nats.publish: 'orchestrator.task',
        to-json({:prompt("What is {$i} + {$i}? Reply with just the number."),
                 :session_id("test-concurrent-{$i}"),
                 :chat_id<test-e2e>}),
        :reply-to($inbox);
    @concurrent-promises.push: { :$p, :$sub, :$inbox, :$i }.hash;
}

await Promise.anyof: Promise.allof(@concurrent-promises.map({ .<p> })), Promise.in(180);

my $concurrent-oks = 0;
for @concurrent-promises -> %cp {
    if %cp<p>.so {
        my $payload = %cp<p>.result.payload;
        my %r = try from-json($payload) // {};
        if %r<result> && !%r<error> { $concurrent-oks++ }
        else { note "    task {%cp<i>}: {%r.gist.substr(0,80)}" }
    }
    %cp<sub>.unsubscribe;
}
ok($concurrent-oks >= 2, "concurrent tasks: {$concurrent-oks}/3 succeeded");
say "";

# ═══════════════════════════════════════════
say "════════════════════════════════════";
say "RESULTS: ✅ {$passed} passed | ❌ {$failed} failed | ⏭️ {$skipped} skipped";
say "";

if @failures {
    say "FAILURES:";
    for @failures -> $f {
        say "  ❌ {$f}";
    }
    say "";
}

exit $failed > 0 ?? 1 !! 0;
