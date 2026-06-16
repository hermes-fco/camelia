#!/usr/bin/env raku
# 🌺 Camélia — Web Browser Worker (task-store pull)
#
# Claims tasks from task-store via task.store.next,
# fetches URLs with curl + optional chromium JS rendering,
# updates task-store with results.

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url     = %*ENV<NATS_URL>     // 'nats://127.0.0.1:4222';
my $service-name = %*ENV<SERVICE_NAME> // 'worker-web-browser';
my $worker-id    = ('w' ~ (^10000).pick).Str;

note "🟡 {$service-name}-{$worker-id} connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 {$service-name}-{$worker-id} connected.";

# ── Lifecycle event publisher ──
my $lifecycle-subject = "worker.status.web-browser.{$worker-id}";
sub lifecycle(Str $event) {
    $nats.publish: "{$lifecycle-subject}.{$event}",
        to-json({ :$worker-id, :type<web-browser>, :$event, :ts(now.Real) });
}

# ── NATS request-reply helper ──
sub nats-request(Str $subject, Str $payload, Int :$timeout = 30 --> Hash) {
    my $sub = $nats.subscribe: my $inbox = "_INBOX.wbr." ~ (^1_000_000).pick, :1max-messages;
    my $p   = $sub.supply.head.Promise;
    $nats.publish: $subject, $payload, :reply-to($inbox);
    await Promise.anyof: $p, Promise.in($timeout);
    $sub.unsubscribe;
    return { :error("No response") } unless $p.so;
    my $msg = $p.result;
    return { :error("Empty response") } unless $msg && $msg.payload;
    try from-json($msg.payload) // { :error("JSON parse fail") };
}

# ── Check for headless browser ──
sub find-headless-browser(--> Str) {
    for <chromium-browser chromium google-chrome google-chrome-stable> -> $bin {
        my $proc = Proc::Async.new('which', $bin);
        my $out = '';
        $proc.stdout.lines(:chomp).tap(-> $l { $out ~= $l });
        my $r = await $proc.start;
        return $bin if $r.exitcode == 0 && $out.chars > 0;
    }
    return '';
}

my $headless-bin = find-headless-browser();
note $headless-bin
    ?? "🟢 Headless browser: {$headless-bin}"
    !! "🟡 No headless browser found — JS rendering disabled";

# ── curl fetch ──
sub curl-fetch(Str $url, Int :$timeout = 15 --> Hash) {
    my $proc = Proc::Async.new(
        'curl', '-sL',
        '--connect-timeout', '10',
        '--max-time', $timeout.Str,
        '-w', "\n%\{http_code\}",
        $url
    );
    my $output = '';
    $proc.stdout.lines(:chomp).tap(-> $l { $output ~= "$l\n" });
    my $result = await $proc.start;
    return { :ok(False), :error("curl exit={$result.exitcode}") } if $result.exitcode != 0;
    my @lines = $output.lines;
    my $status = @lines.pop // '0';
    $status ~~ s/^\s+|\s+$//;
    my $content = @lines.join("\n");
    if $status ~~ /^2/ {
        return { :ok(True), :$content, :status($status.Int) };
    }
    return { :ok(False), :error("HTTP {$status}"), :status($status.Int), :$content };
}

# ── chromium render ──
sub chromium-render(Str $url, Int :$timeout = 15 --> Hash) {
    unless $headless-bin {
        my %result = curl-fetch($url, :$timeout);
        %result<js_rendered> = False;
        %result<note> = "JS not rendered (no headless browser available)";
        return %result;
    }
    my $proc = Proc::Async.new(
        $headless-bin,
        '--headless', '--disable-gpu', '--no-sandbox',
        '--dump-dom',
        '--virtual-time-budget=' ~ ($timeout * 1000).Str,
        $url
    );
    my $output = '';
    $proc.stdout.lines(:chomp).tap(-> $l { $output ~= "$l\n" });
    my $result = await $proc.start;
    if $result.exitcode != 0 {
        note "⚠️ chromium failed, falling back to curl";
        my %fallback = curl-fetch($url, :$timeout);
        %fallback<js_rendered> = False;
        %fallback<note> = "chromium failed, fell back to curl";
        return %fallback;
    }
    return { :ok(True), :content($output), :status(200), :js_rendered(True) };
}

# ── HTML to text ──
sub html-to-text(Str $html --> Str) {
    my $text = $html;
    $text ~~ s:g:s/'<script' .*? '</script>'//;
    $text ~~ s:g:s/'<style'  .*? '</style>' //;
    $text ~~ s:g/'<' <-[>]>* '>'//;
    $text ~~ s:g/'&amp;' /&/;
    $text ~~ s:g/'&lt;'  /</;
    $text ~~ s:g/'&gt;'  />/;
    $text ~~ s:g/'&quot;'/\"/;
    $text ~~ s:g/'&apos;'/'/;
    $text ~~ s:g/'&nbsp;'/ /;
    $text ~~ s:g/^^ \s+//;
    $text ~~ s:g/\n\n\n+/\n\n/;
    return $text;
}

# ── Extract title from HTML ──
sub extract-title(Str $html --> Str) {
    if $html ~~ /:s '<title>' (.*?) '</title>'/ { return $0.Str }
    return '';
}

# ── Handle task ──
my $last-activity = now;

sub handle-task(%task --> Hash) {
    my $desc = %task<description> // '';
    note "📨 {$service-name}-{$worker-id}: {$desc.substr(0, 80)}...";

    # Determine if it's a URL or a search query
    my $url = $desc;
    if $desc ~~ /^ 'curl '/ {
        note "  ⚠️ curl command received, extracting URL...";
        $url = $desc.subst(/^ 'curl ' .*? ' '/, '').subst(/\s+.*$/, '');
    }

    my %result = curl-fetch($url, :timeout(20));

    # If we have a headless browser and curl got a 200, try JS rendering too
    if %result<ok> && $headless-bin {
        note "  🔄 Trying JS rendering with chromium...";
        my %js-result = chromium-render($url, :timeout(25));
        if %js-result<ok> {
            note "  ✅ JS render ok ({%js-result<content>.chars} chars)";
            my $text  = html-to-text(%js-result<content>);
            my $title = extract-title(%js-result<content>);
            %result = %( :ok(True), :$title, :$text, :status(200), :js_rendered(True) );
        }
    } elsif %result<ok> {
        my $text  = html-to-text(%result<content>);
        my $title = extract-title(%result<content>);
        %result = %( :ok(True), :$title, :$text, :status(%result<status>), :js_rendered(False) );
    }

    %result<worker-id> = $worker-id;
    %result<url> = $url;
    return %result;
}

# Health check
my $health-sub = $nats.subscribe: 'health.check.web.browser';

# ═════════════════════════════════════════════
# MAIN LOOP — poll task-store for tasks
# ═════════════════════════════════════════════

note "🔄 Task polling loop ready — {$service-name}-{$worker-id}";

start {
    sleep 0.5;  # let react start first
    lifecycle('started');

    loop {
        # Claim next pending task for web-browser type
        my %resp = nats-request('task.store.next',
            to-json({ :worker_type('web-browser') }), :timeout(10));

        unless %resp<ok> && %resp<task> {
            sleep 2;
            next;
        }

        my %task = %resp<task>;
        my $task-id = %task<id> // '';
        note "📨 {$service-name}-{$worker-id}: claimed {$task-id}";

        $last-activity = now;
        lifecycle('busy');

        try {
            my %result = handle-task(%task);
            my $result-str = to-json(%result);

            nats-request('task.store.update', to-json({
                :id($task-id), :status<completed>, :result($result-str),
            }), :timeout(10));
            note "  ✅ {$task-id} completed";
        }

        CATCH {
            default {
                note "  ❌ Task {$task-id} crashed: {.message}";
                try nats-request('task.store.update', to-json({
                    :id($task-id), :status<failed>, :error_msg(.message),
                }), :timeout(10));
            }
        }

        $last-activity = now;
        lifecycle('idle');
    }
}

react {
    # ── Health check (with idle_seconds for spawner GC) ──
    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service<web.browser>,
                :headless_browser($headless-bin // 'none'),
                :$worker-id,
                :last_activity($last-activity.Real),
                :idle_seconds((now - $last-activity).Int),
            });
        }
    }
}
