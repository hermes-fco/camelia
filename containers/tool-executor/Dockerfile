FROM python:3.13-slim

RUN pip install nats-py

COPY containers/tool-executor/service.py /app/service.py
WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/service.py"]
