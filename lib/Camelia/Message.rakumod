# Camelia::Message — JSON message encoding/decoding
unit module Camelia::Message;

use JSON::Fast;

class Message {
    has Str  $.id;
    has Str  $.type;
    has      $.data;
    has Str  $.reply-to;

    method to-json(--> Str) {
        my %h;
        %h<id>       = $!id       if $!id;
        %h<type>     = $!type     if $!type;
        %h<data>     = $!data     if $!data.defined;
        %h<reply-to> = $!reply-to if $!reply-to;
        to-json(%h);
    }

    method from-json(Str $json --> Message) {
        my %h = from-json($json);
        self.new:
            :id(%h<id>),
            :type(%h<type>),
            :data(%h<data>),
            :reply-to(%h<reply-to>),
    }
}

# Shortcut: encode any hash to JSON
sub encode($data --> Str) is export {
    to-json($data);
}

# Shortcut: decode JSON to hash
sub decode(Str $json --> Hash) is export {
    from-json($json);
}
