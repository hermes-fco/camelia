#!/usr/bin/env raku
# 🌺 Camélia — Web Browser Worker (JetStream pull consumer)
#
# Pulls tasks from WORKER_WEB_BROWSER stream via $consumer.msgs(:batch, :no-wait).
# Lifecycle events published to worker.status.web-browser.<id>.*

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

# ── Worker registry payload (sent from react after orchestrator taps supply) ──
my $registry-msg = to-json({
    :name<web-browser>,
    :subject('worker.web-browser.task.>'),
    :description('Fetches URLs, renders JavaScript, extracts readable text. Use for ANY web/HTTP task'),
    :topics([]),
});

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

sub handle-task(%task, Str $reply-to, $msg) {
    my $task-str = %task<task> // '';
    note "📨 {$service-name}-{$worker-id}: {$task-str.substr(0, 80)}...";

    $last-activity = now;
    lifecycle('busy');

    # Determine if it's a URL or a command
    my $url = $task-str;
    if $task-str ~~ /^ 'curl '/ {
        note "  ⚠️ curl command received, fetching directly...";
        $url = $task-str.subst(/^ 'curl ' .*? ' '/, '').subst(/\s+.*$/, '');
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
            %result = { :ok(True), :$title, :$text, :status(200), :js_rendered(True) };
        }
    } elsif %result<ok> {
        my $text  = html-to-text(%result<content>);
        my $title = extract-title(%result<content>);
        %result = { :ok(True), :$title, :$text, :status(%result<status>), :js_rendered(False) };
    }

    %result<worker-id> = $worker-id;
    $nats.publish: $reply-to, to-json(%result);
    note "  ✅ Result sent to {$reply-to} (" ~ (%result<text> // %result<content> // '').chars ~ " chars)";

    $last-activity = now;
    lifecycle('idle');
}

# ═════════════════════════════════════════════
# JETSTREAM CONSUMER — async pull via .msgs
# ═════════════════════════════════════════════

my $stream-name = 'WORKER_WEB_BROWSER';
note "📥 Creating JetStream consumer on {$stream-name}...";

my $stream = Nats::Stream.new:
    :$nats,
    :name($stream-name),
    ;

my $consumer-name = "{$service-name}-{$worker-id}";
my $consumer = Nats::Consumer.new:
    :$nats,
    :name($consumer-name),
    :stream($stream-name),
    :ack-policy<explicit>,
    :deliver-policy<all>,
    :filter-subject('worker.web-browser.task.>'),
    :max-ack-pending(5),
    :ack-wait(120),
    :replay-policy<instant>,
    ;

my $c-supply = $consumer.create-named;
my $c-msg   = await $c-supply.Promise;
if $c-msg && $c-msg.payload && !$c-msg.payload.starts-with('-ERR') {
    note "  ✅ Consumer {$consumer-name} created";
} else {
    note "  ⚠️ Consumer create: {$c-msg.?payload // 'no response'}";
}

# Health check
my $health-sub = $nats.subscribe: 'health.check.web.browser';

# ═════════════════════════════════════════════
# MAIN REACT LOOP — assíncrono, sem polling
# ═════════════════════════════════════════════

note "🔄 Async pull loop ready — {$consumer-name}";

react {
    # Publish started AFTER react is running (spawner needs time to tap supplies)
    start {
        sleep 0.5;
        $nats.publish: 'worker.registry', $registry-msg;
        lifecycle('started');
    }

    # ── Pull messages from JetStream — :batch, :no-wait (Fernando's pattern) ──
    whenever $consumer.msgs(:batch(5), :no-wait) -> $msg {
        next unless $msg.payload;

        if $msg.payload.starts_with('-ERR') {
            note "⚠️ JetStream: {$msg.payload}";
            last;
        }

        my %task;
        try { %task = from-json($msg.payload) };
        if $! {
            note "⚠️ Invalid JSON payload: {$!.message.substr(0, 80)}";
            $consumer.nak($msg);
            next;
        }

        my $reply-to = %task<reply_to> // '';
        unless $reply-to {
            note "⚠️ Task without reply-to, skipping";
            $consumer.ack($msg);
            next;
        }

        start {
            handle-task(%task, $reply-to, $msg);
            $consumer.ack($msg);
        }
    }

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
