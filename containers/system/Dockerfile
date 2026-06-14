FROM camelia-base:latest

# Install curl for Docker REST API (used instead of docker CLI)
RUN apt-get update -qq && apt-get install -y -qq curl && rm -rf /var/lib/apt/lists/*

COPY containers/system/service.raku /app/service.raku

WORKDIR /app
ENTRYPOINT ["raku", "/app/service.raku"]
