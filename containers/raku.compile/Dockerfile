FROM camelia-base:latest
COPY containers/raku.compile/service.raku /app/service.raku
COPY templates/ /opt/camelia/templates/
ENTRYPOINT ["raku", "/app/service.raku"]
