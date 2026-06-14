FROM camelia-base:latest

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY containers/worker-model-deepseek/service.raku /app/service.raku

ENTRYPOINT ["raku", "/app/service.raku"]
