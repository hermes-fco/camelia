FROM camelia-base:latest

COPY lib/Camelia /opt/camelia/lib/Camelia/
COPY containers/agent/agent.raku /app/agent.raku

ENV SERVICE_NAME=agent
ENTRYPOINT ["raku", "/app/agent.raku"]
