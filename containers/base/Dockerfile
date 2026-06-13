# 🌺 Camélia Base Image — Rakudo 2026.05 via rakubrew

FROM debian:13-slim

# Build deps + runtime + perl (rakubrew is a Perl script)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl git ca-certificates procps perl \
    && rm -rf /var/lib/apt/lists/*

# Install rakubrew Perl script directly from releases
ENV RAKUBREW_HOME=/opt/rakubrew \
    HOME=/root
RUN mkdir -p $RAKUBREW_HOME/bin \
    && curl -sL -o $RAKUBREW_HOME/bin/rakubrew \
       https://rakubrew.org/files/45/perl/rakubrew \
    && chmod +x $RAKUBREW_HOME/bin/rakubrew

# Raku 2026.05
ENV PATH=$RAKUBREW_HOME/bin:$RAKUBREW_HOME/versions/moar-2026.05/bin:$RAKUBREW_HOME/shims:$HOME/.rakubrew/versions/moar-2026.05/bin:$HOME/.rakubrew/shims:$PATH
RUN $RAKUBREW_HOME/bin/rakubrew mode shim \
    && $RAKUBREW_HOME/bin/rakubrew download moar-2026.05 \
    && $RAKUBREW_HOME/bin/rakubrew global moar-2026.05 \
    && $RAKUBREW_HOME/bin/rakubrew build-zef

# zef modules
RUN zef install URL JSON::Fast HTTP::Tinyish

# Verify
RUN raku -v

# nats.raku
COPY lib/nats.raku /opt/nats.raku/
RUN raku -I /opt/nats.raku -e 'use Nats; say "✅ NATS OK"'

# Camelia lib
COPY lib/Camelia /opt/camelia/lib/Camelia/

ENV RAKULIB=/opt/nats.raku,/opt/camelia/lib
ENV SERVICE_NAME=camelia
WORKDIR /app
