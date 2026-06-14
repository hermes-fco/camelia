#!/usr/bin/env raku
# 🌺 Camélia — Tool Executor (Raku)
# Executes shell commands, reads/writes files in the sandbox

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url   = %*ENV<NATS_URL>      // 'nats://127.0.0.1:4222';
my $sandbox-dir = %*ENV<SANDBOX_DIR>  // '/tmp/sandbox';

$sandbox-dir.IO.mkdir: :parents;

note "🟡 Connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 NATS connected.";

# Subscribe to tools.exec.> (NATS wildcard)
my $sub = $nats.subscribe: 'tools.exec.>';
note "🟢 Waiting for tool calls on tools.exec.> (sandbox=$sandbox-dir)...";

# Health check
my $health-sub = $nats.subscribe: 'health.check.tool-executor';

# ── Tools ──

sub run-shell(Str $command, Int :$timeout = 30 --> Hash) {
    my $proc = shell(:out, :err, "cd {$sandbox-dir} && {$command} 2>&1");
    my $output = $proc.out.slurp: :close;
    my $exit   = $proc.exitcode;
    {
        :stdout($output.substr(0, 5000)),
        :exit_code($exit),
    }
}

sub read-file(Str $path, Int :$offset = 0, Int :$limit = 500 --> Hash) {
    my $full-path = $sandbox-dir.IO.add($path);

    unless $full-path.resolve.Str.starts-with($sandbox-dir.IO.resolve.Str) {
        return { :error("Path traversal denied: $path") };
    }

    unless $full-path.e {
        return { :error("File not found: $path") };
    }

    my @lines = $full-path.lines;
    my $total = @lines.elems;
    my @sliced = @lines[$offset ..^ min($offset + $limit, $total)];

    {
        :content(@sliced.join("\n")),
        :total_lines($total),
        :offset($offset),
        :limit($limit),
    }
}

sub write-file(Str $path, Str $content --> Hash) {
    my $full-path = $sandbox-dir.IO.add($path);

    unless $full-path.resolve.Str.starts-with($sandbox-dir.IO.resolve.Str) {
        return { :error("Path traversal denied: $path") };
    }

    $full-path.parent.mkdir: :parents;
    $full-path.spurt: $content;
    {
        :ok(True),
        :path($full-path.Str),
        :bytes($content.encode('utf8').bytes),
    }
}

my %tools = %(
    :run_shell(&run-shell),
    :read_file(&read-file),
    :write_file(&write-file),
);

# ── Main loop via react/whenever ──

react {
    whenever $sub.supply -> $msg {
        next unless $msg.payload;

        my $reply-to = $msg.reply-to;
        unless $reply-to {
            note "⚠️ No reply-to, dropping";
            next;
        }

        my %req = try from-json($msg.payload);
        if $! {
            $nats.publish: $reply-to, to-json({ :error("Invalid JSON") });
            next;
        }

        my $tool-name    = %req<name> // '';
        my $tool-call-id = %req<tool_call_id> // 'unknown';
        my %args         = %req<arguments> // {};

        note "🔧 Executing {$tool-name} (id={$tool-call-id})";

        my $result;
        if %tools{$tool-name}:exists {
            try {
                # DeepSeek sends named args; treat first value as positional
                my @values = %args.values;
                $result = %tools{$tool-name}(|@values);
                CATCH {
                    default {
                        $result = %( :error(.message) );
                    }
                }
            }
        } else {
            $result = %( :error("Unknown tool: $tool-name") );
        }

        my $response = to-json({
            :tool_call_id($tool-call-id),
            :name($tool-name),
            :result($result),
        });

        $nats.publish: $reply-to, $response;
    }

    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({ :status<ok>, :service<tool-executor> });
        }
    }
}
