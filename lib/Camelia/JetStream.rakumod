# Camelia::JetStream — Stream & consumer management
unit module Camelia::JetStream;

use Nats;
use Nats::JetStream;
use Camelia::Message;

class StreamManager {
    has $.nats;

    # Ensure a stream exists (create if not, ignore "already exists")
    method ensure(Str $name, :@subjects, :$max-age = 0) {
        my $stream = $!nats.stream: $name, |(:@subjects if @subjects);
        my $resp = await $stream.create;
        my $payload = $resp.payload // '';
        # "already exists" is OK, any other error is real
        if $payload && $payload.starts-with('-ERR') && $payload !~~ /'already exists'/ {
            die "Stream create failed: $payload";
        }
        $stream
    }
}

class PullConsumer {
    has $.consumer;          # NATS::Consumer
    has $.subscription;

    method new(:$stream, :$durable, :$subject) {
        my $c = $stream.consumer: $durable, :filter-subject($subject);
        my $resp = await $c.create-named;
        my $payload = $resp.payload // '';
        # Ignore "already exists" for durable consumers
        if $payload && $payload.starts-with('-ERR') && $payload !~~ /'already exists'/ {
            die "Consumer create failed: $payload";
        }
        self.bless: :consumer($c)
    }

    # Fetch next message (blocking with timeout in seconds)
    method fetch(UInt :$timeout = 60) {
        my $resp = await $!consumer.next: :expires($timeout * 1_000_000_000);
        return Nil unless $resp && $resp.payload && $resp.payload.chars;
        return Nil if $resp.payload.starts-with('-ERR');
        Camelia::Message::Message.from-json($resp.payload);
    }

    method ack($msg) {
        # msg is Nats::Message from the original response
        $!consumer.ack: $_ if $msg.^can('reply-to') && $msg.reply-to
    }

    # Number of pending messages (via consumer info)
    method pending(--> Int) {
        my $resp = await $!consumer.info;
        return 0 unless $resp && $resp.payload;
        my %info = Camelia::Message::decode($resp.payload);
        %info<num_pending> // %info<num_waiting> // 0
    }
}
