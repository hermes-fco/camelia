FROM camelia-base:latest

COPY containers/session-store/session-store.raku /app/session-store.raku

WORKDIR /app
ENTRYPOINT ["raku", "/app/session-store.raku"]
