FROM camelia-base:latest

# SQLite CLI
RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

COPY task-store.raku /app/task-store.raku

# Persistent volume for SQLite DB
VOLUME /data

WORKDIR /app
ENTRYPOINT ["raku", "/app/task-store.raku"]
