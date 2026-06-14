FROM camelia-base:latest
RUN apt-get update -qq && apt-get install -y -qq chromium 2>/dev/null || true
COPY containers/web-browser/service.raku /app/service.raku
ENTRYPOINT ["raku", "/app/service.raku"]
