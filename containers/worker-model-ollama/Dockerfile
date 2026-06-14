FROM camelia-base:latest

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Camelia shared library (loaded via -I, not RAKULIB — avoids Nats shadowing)
COPY lib/Camelia/ /opt/camelia/lib/Camelia/

# Service
COPY containers/worker-model-ollama/service.raku /app/service.raku

ENV RAKULIB=/opt/nats.raku

ENTRYPOINT ["raku", "-I/opt/camelia/lib", "/app/service.raku"]
