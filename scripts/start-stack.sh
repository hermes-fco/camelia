#!/bin/bash
# 🌺 Camélia — Startup Script (replaces docker-compose)
set -e

NETWORK="camelia-net"
DEEPSEEK_KEY="${DEEPSEEK_API_KEY:-}"

echo "🌺 Camélia PoC #7 — Startup"

# Cleanup any existing
docker rm -f camelia-spawner camelia-orchestrator camelia-session-store \
           camelia-tool-executor camelia-model-deepseek camelia-nats 2>/dev/null || true
docker network rm "$NETWORK" 2>/dev/null || true

docker network create "$NETWORK"

# 1. NATS (core dependency)
echo "🟡 Starting NATS..."
docker run -d --name camelia-nats --network "$NETWORK" \
  nats:latest --jetstream

# Wait for NATS
echo "⏳ Waiting for NATS..."
for i in $(seq 1 30); do
  docker exec camelia-nats nats-server --version 2>/dev/null && break
  sleep 1
done
echo "🟢 NATS ready"

# 2. Model-DeepSeek
echo "🟡 Starting model-deepseek..."
docker run -d --name camelia-model-deepseek --network "$NETWORK" \
  -e NATS_URL=nats://camelia-nats:4222 \
  -e DEEPSEEK_API_KEY="$DEEPSEEK_KEY" \
  camelia-model-deepseek:latest

# 3. Tool Executor
echo "🟡 Starting tool-executor..."
docker run -d --name camelia-tool-executor --network "$NETWORK" \
  -e NATS_URL=nats://camelia-nats:4222 \
  camelia-tool-executor:latest

# 4. Session Store
echo "🟡 Starting session-store..."
docker run -d --name camelia-session-store --network "$NETWORK" \
  -e NATS_URL=nats://camelia-nats:4222 \
  camelia-session-store:latest

# 5. Orchestrator
echo "🟡 Starting orchestrator..."
docker run -d --name camelia-orchestrator --network "$NETWORK" \
  -e NATS_URL=nats://camelia-nats:4222 \
  -e SERVICE_NAME=orchestrator \
  -e MAX_WORKERS=3 \
  camelia-orchestrator:latest

# 6. Spawner
echo "🟡 Starting spawner..."
docker run -d --name camelia-spawner --network "$NETWORK" \
  -e NATS_URL=nats://camelia-nats:4222 \
  -e MAX_WORKERS=3 \
  -e WORKER_IMAGE=camelia-worker:latest \
  -v /var/run/docker.sock:/var/run/docker.sock \
  camelia-spawner:latest

sleep 3
echo ""
echo "=== All containers ==="
docker ps --filter "name=camelia-" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""
echo "🟢 Stack started!"
