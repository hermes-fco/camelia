#!/usr/bin/env raku
# 🌺 Camélia — Worker: Model DeepSeek (LLM proxy via HTTP API)
# Common logic in Camelia::ModelWorker — this file only provides the API call.

use Camelia::ModelWorker;

my $model = %*ENV<DEEPSEEK_MODEL> // 'deepseek-v4-pro';

# ── Provider-specific: DeepSeek API call via curl ──
my $auth = "Authorization: " ~ "Bearer " ~ %*ENV<DEEPSEEK_API_KEY>;
spurt('/tmp/auth_header', $auth);

sub deepseek-api(Str $body --> Str) {
    my $proc = Proc::Async.new(
        'curl', '-s',
        '--connect-timeout', '30',
        '--max-time', '120',
        'https://api.deepseek.com/v1/chat/completions',
        '-H', '@' ~ '/tmp/auth_header',
        '-H', 'Content-Type: application/json',
        '-d', $body,
    );
    my $output = '';
    $proc.stdout.lines(:chomp).tap(-> $line { $output ~= $line ~ "\n" });
    await $proc.start;
    return $output;
}

# ── Inject model name into every request ──
my &inject-model = sub (Str $body --> Str) {
    my %parsed = try from-json($body);
    if !$! && %parsed {
        %parsed<model> = $model unless %parsed<model>:exists;
        return deepseek-api(to-json(%parsed));
    }
    deepseek-api($body);
};

run-model-worker(
    :subject('worker.model.deepseek.completion'),
    :queue('worker-model-deepseek'),
    :call-api(&inject-model),
);
