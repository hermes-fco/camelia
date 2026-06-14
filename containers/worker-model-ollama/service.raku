#!/usr/bin/env raku
use JSON::Fast;
use Camelia::ModelWorker;

my $ollama-url = %*ENV<OLLAMA_URL>   // 'http://ollama:11434';
my $model      = %*ENV<OLLAMA_MODEL>  // 'qwen2.5:3b';
my $safe-model = $model.subst(':', '-', :g);

sub ollama-api(Str $body is copy --> Str) {
    # Inject model and stream:false via string subst
    my $prefix = '{"stream":false,"model":"' ~ $model ~ '",';
    $body = $body.subst(/^ '{'/, $prefix);

    my $proc = Proc::Async.new(
        'curl', '-s', '--connect-timeout', '30', '--max-time', '300',
        "{$ollama-url}/v1/chat/completions",
        '-H', 'Content-Type: application/json', '-d', $body,
    );
    my $output = '';
    $proc.stdout.lines(:chomp).tap(-> $line { $output ~= $line ~ "\n" });
    await $proc.start;
    return $output;
}

run-model-worker(
    :subject("worker.model.ollama.{$safe-model}.completion"),
    :queue('worker-model-ollama'),
    :call-api(&ollama-api),
);
