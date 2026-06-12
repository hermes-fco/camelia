FROM python:3.13-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN pip install nats-py

COPY containers/model-deepseek/service.py /app/service.py
WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/service.py"]
