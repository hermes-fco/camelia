FROM camelia-base:latest

# Install docker CLI (for building worker images)
RUN apt-get update -qq && apt-get install -y -qq docker-cli && rm -rf /var/lib/apt/lists/*

COPY containers/worker-factory/worker-factory.raku /app/worker-factory.raku
COPY templates/ /opt/camelia/templates/

WORKDIR /app
ENTRYPOINT ["raku", "/app/worker-factory.raku"]
