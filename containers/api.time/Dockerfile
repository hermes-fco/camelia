FROM camelia-base:latest
COPY containers/api.time/service.raku /app/service.raku
COPY templates/ /opt/camelia/templates/
ENTRYPOINT ["raku", "/app/service.raku"]
