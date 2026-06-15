#!/usr/bin/env raku
# 🌺 Camélia — Web Browser Worker
#
# Fetches URLs via curl, with optional chromium headless for JS rendering,
# plus HTML-to-text extraction.

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222';

note "🟡 web.browser connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 web.browser connected.";

my $task-sub   = $nats.subscribe: 'worker.web-browser.task.>';
my $health-sub = $nats.subscribe: 'health.check.web.browser';
note "🟢 Listening on web.browser.>";

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
        # Fallback to curl
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

    # Remove script and style blocks
    $text ~~ s:g:s/'<script' .*? '</script>'//;
    $text ~~ s:g:s/'<style'  .*? '</style>' //;

    # Extract title
    my $title = '';
    if $text ~~ /:s '<title>' (.*?) '</title>'/ { $title = $0.Str }

    # Remove all tags
    $text ~~ s:g/'<' <-[>]>* '>'//;

    # Decode common entities
    $text ~~ s:g/'&amp;' /&/;
    $text ~~ s:g/'&lt;'  /</;
    $text ~~ s:g/'&gt;'  />/;
    $text ~~ s:g/'&quot;'/\"/;
    $text ~~ s:g/'&apos;'/\'/;
    $text ~~ s:g/'&nbsp;'/ /;

    # Collapse whitespace
    $text ~~ s:g/\s+/ /;
    $text .= trim;

    return $text;
}

# ═══ Tools ═══
#   fetch   — curl-based fetch, no JS
#   render  — headless chromium (or curl fallback)
#   extract — fetch + strip HTML to plain text

react {
    whenever $task-sub.supply -> $msg {
        next unless $msg.payload;
        my $reply-to = $msg.?reply-to;
        unless $reply-to {
            note "⚠️ No reply-to, ignoring";
            next;
        }

        my %task = try from-json($msg.payload);
        if $! {
            $nats.publish: $reply-to, to-json({ :error("Invalid JSON") });
            next;
        }

        my $action = %task<action> // '';
        note "📨 web.browser: {$action}";

        start {
            my %result = handle-task(%task);
            $nats.publish: $reply-to, to-json(%result);
        }
    }

    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service<web.browser>,
                :headless_browser($headless-bin // 'none'),
            });
        }
    }
}

sub handle-task(%task --> Hash) {
    # If no 'action' but has 'task', auto-detect: URLs → extract, commands → shell
    if !%task<action> && %task<task> {
        my $cmd = %task<task>;
        if $cmd.contains('https://') || $cmd.contains('http://') {
            # Extract URL and auto-extract (fetch + strip HTML)
            if $cmd ~~ /(https? '://' \S+)/ {
                %task<url> = $0.Str;
                %task<action> = 'extract';
            }
        }
        # If still no action (URL regex failed or no URL found), run as shell
        unless %task<action> {
            my $proc = Proc::Async.new('sh', '-c', $cmd);
            my ($out, $err) = ('', '');
            $proc.stdout.lines(:chomp).tap(-> $l { $out ~= "$l\n" });
            $proc.stderr.lines(:chomp).tap(-> $l { $err ~= "$l\n" });
            my $r = await $proc.start;
            return { :ok($r.exitcode == 0), :stdout($out), :stderr($err), :exit($r.exitcode) };
        }
    }
    given %task<action> // '' {
        when 'fetch' {
            my $url     = %task<url> // '';
            return { :error("Missing 'url'") } unless $url;
            my $timeout = %task<timeout> // 15;
            return curl-fetch($url, :$timeout);
        }
        when 'render' {
            my $url     = %task<url> // '';
            return { :error("Missing 'url'") } unless $url;
            my $timeout = %task<timeout> // 15;
            return chromium-render($url, :$timeout);
        }
        when 'extract' {
            my $url     = %task<url> // '';
            return { :error("Missing 'url'") } unless $url;
            my $timeout = %task<timeout> // 15;

            my %fetch = curl-fetch($url, :$timeout);
            return %fetch unless %fetch<ok>;

            my $text  = html-to-text(%fetch<content>);
            my $title = '';
            if %fetch<content> ~~ /:s '<title>' (.*?) '</title>'/ {
                $title = $0.Str;
            }

            return { :ok(True), :$text, :$title, :status(%fetch<status>) };
        }
        default {
            return { :error("Unknown action '{%task<action>}'. Available: fetch, render, extract") };
        }
    }
}
