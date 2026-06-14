FROM camelia-base:latest

COPY containers/worker-factory/worker-factory.raku /app/worker-factory.raku
COPY templates/ /opt/camelia/templates/

WORKDIR /app
ENTRYPOINT ["raku", "/app/worker-factory.raku"]
