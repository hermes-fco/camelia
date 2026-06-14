FROM camelia-base:latest
COPY containers/entry-factory/entry-factory.raku /app/entry-factory.raku
COPY templates/ /opt/camelia/templates/
WORKDIR /app
ENTRYPOINT ["raku", "/app/entry-factory.raku"]
