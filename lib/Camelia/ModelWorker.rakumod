# 🌺 Camelia::ModelWorker — Common base for all LLM proxy workers
#
# Every model provider does the same thing:
#   1. Connect to NATS
#   2. Subscribe to <subject> with queue group <queue>
#   3. Add a health check subscription
#   4. Enter a react loop: receive request → call HTTP API → publish reply
#
# Only the API call differs between providers.
#
# Usage:
#   use Camelia::ModelWorker;
#   sub my-api(Str $body --> Str) { ... }  # provider-specific HTTP call
#   run-model-worker(
#       :subject('worker.model.foo.completion'),
#       :queue('worker-model-foo'),
#       :&call-api(&my-api),
#   );

use Nats;
use JSON::Fast;

sub run-model-worker(
    Str :$subject!,
    Str :$queue!,
    :&call-api!,  # Str → Str
    Str :$nats-url = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222',
) is export {
    $*ERR.out-buffer = False;

    # ── Connect NATS ──
    note "🟡 Worker connecting NATS ($nats-url)...";
    my $nats = Nats.new: :servers[$nats-url];
    await $nats.start;
    $nats.connect;
    note "🟢 NATS connected.";

    # ── Subscribe with queue group (auto-scale via NATS round-robin) ──
    my $sub = $nats.subscribe: $subject, :$queue;
    note "🟢 Subscribed {$subject} (queue: {$queue}), SID={$sub.sid}. Entering react...";

    # ── Health check ──
    my $service-name = $queue.subst('worker-', '');  # worker-model-deepseek → model-deepseek
    my $health-sub = $nats.subscribe: "health.check.worker.{$service-name}";
    note "🩺 Health sub SID={$health-sub.sid}";

    # ── Main loop ──
    react {
        whenever $sub.supply -> $msg {
            note "📨 MSG RECEIVED! payload={$msg.payload.chars} chars";
            next unless $msg.payload;

            my %req = try from-json($msg.payload);
            if $! { note "❌ JSON parse: $!"; next; }
            my $request-id = %req<id> // 'unknown';
            my $reply-to   = $msg.?reply-to;
            note "📨 Prompt received (id=$request-id, reply-to={$reply-to // 'NONE'})";

            # start {} keeps react loop responsive for health checks
            start {
                my $body = to-json(%req<messages>:exists ?? %req !! %req<messages>);

                # If the caller passes raw messages array without wrapping,
                # wrap it now
                if %req<messages> {
                    my %api-body = %( :messages(%req<messages>) );
                    %api-body<tools> = %req<tools> if %req<tools>:exists;
                    %api-body<tool_choice> = %req<tool_choice> if %req<tool_choice>:exists;
                    %api-body<temperature> = %req<temperature> if %req<temperature>:exists;
                    # Model name injected by caller or from env
                    %api-body<model> = %req<model> if %req<model>:exists;
                    $body = to-json(%api-body);
                }

                my $resp = '';
                my $attempts = 3;
                for 1 .. $attempts -> $attempt {
                    note "DEBUG: calling API (attempt {$attempt}/{$attempts})...";
                    $resp = call-api($body);
                    note "DEBUG: API done, len={$resp.chars}";
                    last if $resp ~~ /^ '{' /;
                    if $attempt < $attempts {
                        my $delay = 2 ** ($attempt - 1);
                        note "  ⚠️ Non-JSON response, retrying in {$delay}s...";
                        sleep $delay;
                    }
                }

                if $resp !~~ /^ '{' / {
                    note "❌ Non-JSON after {$attempts} attempts: {$resp.substr(0, 200)}";
                    $nats.publish: $reply-to, to-json({ :error("API failed after {$attempts} retries") }) if $reply-to;
                }
                else {
                    my $data = try from-json($resp);
                    if $! {
                        note "❌ JSON parse: $!";
                        $nats.publish: $reply-to, to-json({ :error("JSON parse failed") }) if $reply-to;
                    }
                    elsif $data<error> {
                        note "❌ API error: {$data<error><message> // $data<error>}";
                        $nats.publish: $reply-to, to-json({ :error($data<error><message> // $data<error>) }) if $reply-to;
                    }
                    else {
                        my $choice  = $data<choices> // [];
                        if $choice ~~ Array && $choice.elems > 0 {
                            $choice = $choice[0];
                        }
                        my $content = $choice<message><content> // $choice<text> // '';
                        my $usage   = $data<usage>;
                        my $finish  = $choice<finish_reason> // 'stop';

                        if $content { note "💬 {$content.substr(0, 100)}..." }
                        note "✅ finish={$finish}, tokens={$usage<total_tokens> // '?'}";

                        if $reply-to {
                            $nats.publish: $reply-to, to-json $data;
                            note "DEBUG: published reply";
                        }
                    }
                }
            }
        }

        whenever $health-sub.supply -> $msg {
            my $rt = $msg.?reply-to;
            if $rt {
                $nats.publish: $rt, to-json({ :status<ok>, :service("worker-{$service-name}") });
            }
        }
    }
}
