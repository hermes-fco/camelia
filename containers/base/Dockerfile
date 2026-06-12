# 🌺 Camélia Base Image

FROM debian:13-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    rakudo raku-zef raku-json-fast curl git procps ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN zef install URL HTTP::Tinyish

# nats.raku: lib/ → /opt/nats.raku/
COPY lib/nats.raku /opt/nats.raku/
RUN raku -I /opt/nats.raku -e 'use Nats; say "✅ NATS OK"'

# Camelia lib
COPY lib/Camelia /opt/camelia/lib/Camelia/

ENV RAKULIB=/opt/nats.raku,/opt/camelia/lib
ENV SERVICE_NAME=camelia
WORKDIR /app
