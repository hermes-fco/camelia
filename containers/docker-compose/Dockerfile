FROM camelia-base:latest

# Install Docker CLI + docker-compose standalone
RUN apt-get update -qq \
    && apt-get install -y -qq docker.io docker-compose curl \
    && rm -rf /var/lib/apt/lists/*

# Project directory for compose file (mounted writable at runtime)
RUN mkdir -p /camelia
COPY containers/docker-compose/service.raku /app/service.raku

WORKDIR /app
ENTRYPOINT ["raku", "/app/service.raku"]
