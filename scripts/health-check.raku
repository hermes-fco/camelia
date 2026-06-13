#!/usr/bin/env raku
# Health check script - runs inside a container on camelia-net
use Nats;
use JSON::Fast;

my $nats = Nats.new: :servers['nats://camelia-nats:4222'];
await $nats.start;
$nats.connect;

my @services = <model-deepseek tool-executor session-store orchestrator spawner>;

for @services -> $svc {
    my $subject = "health.check.{$svc}";
    my $resp = $nats.request($subject, '{}', :timeout(3));
    if $resp {
        my %data = try from-json($resp.payload) // {};
        say "✅ {$svc}: {%data<status> // 'ok'}";
    } else {
        say "❌ {$svc}: no response";
    }
}

$nats.close;
