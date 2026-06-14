FROM camelia-base:latest

COPY containers/skill-store/service.raku /app/service.raku

WORKDIR /app
ENTRYPOINT ["raku", "/app/service.raku"]
