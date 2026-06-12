# Camelia::NATS — NATS connection wrapper
unit module Camelia::NATS;

use Nats;
use Nats::JetStream;

class Connection {
    has $.client;
    has $.jetstream;

    method connect(Str $url = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222') {
        $!client = Nats.new: :servers[$url];
        await $!client.start;
        $!client.connect;
        self
    }

    method publish(Str $subject, Str $payload, :$reply-to) {
        $!client.publish: $subject, $payload, |(:$reply-to with $reply-to)
    }

    method subscribe(Str $subject) {
        $!client.subscribe: $subject
    }

    method request(Str $subject, Str $payload?, :$timeout = 30) {
        $!client.request: $subject, $payload
    }

    method stream(Str $name, *@subjects, |c) {
        $!client.stream: $name, |(:@subjects if @subjects), |c
    }

    method stop {
        $!client.stop
    }
}
