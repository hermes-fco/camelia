#!/usr/bin/env bash
# 🌺 Camélia PoC #5 — Session + Streaming Test
#
# Shows real-time progress by subscribing to:
#   - session.<id>.stream            (orchestrator milestones)
#   - session.<id>.worker.>.progress (worker progress)
# Before sending a task.
#
# Usage:
#   ./scripts/test-session.sh [prompt]
#
# Default prompt: analyze the Camelia project structure

set -euo pipefail

NATS_CONTAINER="${NATS_CONTAINER:-camelia-nats}"
SESSION_ID="sess-$(head -c 6 /dev/urandom | xxd -p)"
INBOX="_INBOX.test.$(head -c 4 /dev/urandom | xxd -p)"
PROMPT="${1:-Analyze the Camelia project structure in /root/camelia: list all containers and their responsibilities.}"

nats() {
    docker exec -i "$NATS_CONTAINER" nats "$@"
}

echo "══════════════════════════════════════════════"
echo "🌺 Camélia PoC #5 — Streaming Test"
echo "══════════════════════════════════════════════"
echo ""
echo "Session:  $SESSION_ID"
echo "Inbox:    $INBOX"
echo "Prompt:   $PROMPT"
echo ""

cleanup() {
    echo ""
    echo "🛑 Stopping subscribers..."
    kill %1 %2 %3 2>/dev/null || true
    exit 0
}
trap cleanup INT TERM

# ── Subscribe to all 3 subjects ──
echo "📡 Subscribing to session streams..."
nats sub "session.${SESSION_ID}.stream" &
nats sub "session.${SESSION_ID}.worker.>.progress" &
nats sub "$INBOX" &

sleep 1

# ── Build JSON payload safely ──
PAYLOAD=$(printf '{"prompt":"%s","session_id":"%s"}' "$PROMPT" "$SESSION_ID")

# ── Publish the task ──
echo ""
echo "📤 Sending task to orchestrator.task ..."
nats pub orchestrator.task "$PAYLOAD" --reply "$INBOX"

echo ""
echo "⏳ Waiting for responses (Ctrl+C to stop)..."
echo ""

wait 2>/dev/null || true
echo ""
echo "✅ Test complete."
