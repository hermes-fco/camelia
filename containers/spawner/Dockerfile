FROM camelia-base:latest

# curl + ca-certificates for Docker REST API calls
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY containers/spawner/spawner.raku /app/spawner.raku

WORKDIR /app
ENTRYPOINT ["raku", "/app/spawner.raku"]
