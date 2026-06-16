#!/usr/bin/env raku
# 🌺 Camélia — Web Search Worker (JetStream pull consumer)
#
# Pulls tasks from WORKER_WEB_SEARCH stream via $consumer.msgs(:batch, :no-wait).
# Takes a search query → fetches DuckDuckGo HTML → extracts results.
# Lifecycle events published to worker.status.web-search.<id>.*

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url     = %*ENV<NATS_URL>     // 'nats://127.0.0.1:4222';
my $service-name = %*ENV<SERVICE_NAME> // 'worker-web-search';
my $worker-id    = ('s' ~ (^10000).pick).Str;

note "🟡 {$service-name}-{$worker-id} connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 {$service-name}-{$worker-id} connected.";

# ── Lifecycle event publisher ──
my $lifecycle-subject = "worker.status.web-search.{$worker-id}";
sub lifecycle(Str $event) {
    $nats.publish: "{$lifecycle-subject}.{$event}",
        to-json({ :$worker-id, :type<web-search>, :$event, :ts(now.Real) });
}

# ── curl fetch ──
sub curl-fetch(Str $url, Int :$timeout = 15 --> Hash) {
    my $proc = Proc::Async.new(
        'curl', '-sL',
        '--connect-timeout', '10',
        '--max-time', $timeout.Str,
        '-A', 'Mozilla/5.0 (compatible; CameliaBot/1.0)',
        '-w', '\n%{http_code}',
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

# ── Search with DuckDuckGo HTML ──
sub ddg-search(Str $query --> Hash) {
    my $encoded = $query.subst(/<-[a..zA..Z0..9_.~-]>/,
        { sprintf('%%%02X', $_.ord) }, :g);
    my $url = "https://html.duckduckgo.com/html/?q={$encoded}";
    note "  🔍 Searching: {$url.substr(0, 100)}...";

    my %fetch = curl-fetch($url, :timeout(20));
    return %fetch unless %fetch<ok>;

    my $html = %fetch<content>;
    note "  📄 Got {($html // '').chars} chars from DDG";

    # Parse DuckDuckGo HTML results
    my @results;

    # DDG HTML uses specific class patterns:
    # <a class="result__a" href="...">Title</a>
    # <a class="result__snippet">Snippet text</a>
    # <a class="result__url">URL display</a>

    # Split by result blocks: each result is a div with class "result"
    my @blocks = $html.split(/ '<div class="result' /);
    @blocks.shift;  # first is before any result

    for @blocks -> $block {
        my $title   = '';
        my $snippet = '';
        my $link    = '';

        # Extract link and title from <a class="result__a">
        if $block ~~ / '<a' .*? 'class="result__a"' .*? 'href="' $<link>=(<-["]>+) '"'
                       .*? '>' $<title>=(.*?) '</a>' / {
            $link  = $<link>.Str;
            $title = $<title>.Str;
            $title ~~ s:g/'<b>'|'</b>'//;  # strip bold tags
            $title ~~ s:g/'<' <-[>]>* '>'//;  # strip any other tags
        }

        # Extract snippet from <a class="result__snippet">
        if $block ~~ / '<a' .*? 'class="result__snippet"' .*? '>' (.*?) '</a>' / {
            $snippet = $0.Str;
            $snippet ~~ s:g/'<b>'|'</b>'//;
            $snippet ~~ s:g/'<' <-[>]>* '>'//;
            $snippet ~~ s:g/'&amp;' /&/;
            $snippet ~~ s:g/'&lt;'  /</;
            $snippet ~~ s:g/'&gt;'  />/;
            $snippet ~~ s:g/'&quot;'/"/;
            $snippet ~~ s:g/'&apos;'/'/;
            $snippet ~~ s:g/'&nbsp;'/ /;
        }

        # Skip empty results
        next unless $title.chars > 0 || $snippet.chars > 0;

        @results.push: { :$title, :$snippet, :$link };
    }

    note "  ✅ Parsed {+@results} results";

    return {
        :ok(True),
        :query($query),
        :results(@results),
        :count(+@results),
        :source<duckduckgo>,
    };
}

# ── HTML to text fallback ──
sub html-to-text(Str $html --> Str) {
    my $text = $html;
    $text ~~ s:g:s/'<script' .*? '</script>'//;
    $text ~~ s:g:s/'<style'  .*? '</style>'//;
    $text ~~ s:g/'<' <-[>]>* '>'//;
    $text ~~ s:g/'&amp;' /&/;
    $text ~~ s:g/'&lt;'  /</;
    $text ~~ s:g/'&gt;'  />/;
    $text ~~ s:g/'&quot;'/"/;
    $text ~~ s:g/'&apos;'/'/;
    $text ~~ s:g/'&nbsp;'/ /;
    $text ~~ s:g/^^ \s+//;
    $text ~~ s:g/\n\n\n+/\n\n/;
    return $text;
}

# ── Handle task ──
my $last-activity = now;

sub handle-task(%task, Str $reply-to, $msg) {
    my $task-str = %task<task> // '';
    note "📨 {$service-name}-{$worker-id}: {$task-str.substr(0, 80)}...";

    $last-activity = now;
    lifecycle('busy');

    # Determine the query
    my $query = $task-str;

    # If the task is already a URL, search with it as a query term
    # (the LLM might send "search for X" — we extract X)
    if $task-str ~~ /:i ^ 'search' \s+ ('for'\s+)? (.*) $/ {
        $query = $1.Str;
    }
    elsif $task-str ~~ /:i ^ 'buscar' \s+ (.*) $/ {
        $query = $1.Str;
    }

    note "  🔎 Query: {$query.substr(0, 80)}";

    my %result = ddg-search($query);

    unless %result<ok> {
        note "  ❌ Search failed: {%result<error>}";
        %result<worker-id> = $worker-id;
        $nats.publish: $reply-to, to-json(%result);
        lifecycle('idle');
        return;
    }

    %result<worker-id> = $worker-id;
    $nats.publish: $reply-to, to-json(%result);
    note "  ✅ Result sent to {$reply-to} ({+%result<results>} results)";

    $last-activity = now;
    lifecycle('idle');
}

# ═════════════════════════════════════════════
# JETSTREAM CONSUMER — async pull via .msgs
# ═════════════════════════════════════════════

my $stream-name = 'WORKER_WEB_SEARCH';
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
    :filter-subject('worker.web-search.task.>'),
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
my $health-sub = $nats.subscribe: 'health.check.web.search';

# ═════════════════════════════════════════════
# MAIN REACT LOOP
# ═════════════════════════════════════════════

note "🔄 Async pull loop ready — {$consumer-name}";

react {
    # Publish started AFTER react is running
    start {
        sleep 0.5;
        lifecycle('started');
    }

    # ── Pull messages from JetStream ──
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

    # ── Health check ──
    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service<web.search>,
                :$worker-id,
                :last_activity($last-activity.Real),
                :idle_seconds((now - $last-activity).Int),
            });
        }
    }
}
