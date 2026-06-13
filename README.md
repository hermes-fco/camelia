# 🌺 Camélia

**Multi-agent AI framework** in Raku with credential isolation via NATS.
**All component communication is exclusively via NATS.**

## Architecture (PoC #5 — Streaming + Session Context)

```
   user / CLI
       │
       │ subscribe: session.<id>.stream  (real-time progress)
       │ subscribe: session.<id>.worker.>.progress
       │
       │ nats pub orchestrator.task '{"prompt":"...","session_id":"sess-abc"}' --reply inbox
       ▼
┌─────────────────┐     ┌──────────┐
│  ORCHESTRATOR   │────▶│  SPAWNER │  subscribe: orchestrator.task
│  (decomposes +  │     │ (docker  │  spawner.control
│   synthesizes +  │     │  socket) │
│   session mgmt)  │     └────┬─────┘
└───┬─────────────┘          │
    │                        │ spawn dynamic workers
    │ worker.task.*          ▼
    │ (JetStream)  ┌──────────────────┐
    │              │ WORKER 1..N      │  pull consumer
    │              │ (ephemeral,      │  JetStream WORKER_TASKS
    │              │  idle timeout    │
    │              │  5 min)          │
    │              └──┬─────┬─────────┘
    │                 │     │
    │     progress ───┘     │
    │     session.<id>.     │
    │     worker.*.progress │
    │                       │
    ▼                       ▼
┌──────────┐     ┌──────────────┐
│  MODEL   │     │    TOOL      │
│ DEEPSEEK │     │  EXECUTOR    │
│ (API key)│     │  (sandbox)   │
└──────────┘     └──────────────┘
     │                  │
     └────── NATS ──────┘
```

### Flow

1. Caller subscribes to `session.<id>.stream` and `session.<id>.worker.>.progress`
2. Caller publishes `orchestrator.task` with `{"prompt": "...", "session_id": "sess-abc"}` + reply-to inbox
3. **Orchestrator** creates/loads session, injects conversation history into prompts
4. Orchestrator streams: `received` → `decomposed` → `workers-ready` → `results-collected` → `synthesizing` → `done`
5. **Workers** stream per-turn progress: `started` → `thinking` → `tool_call` → `finished`
6. Final response sent to caller's reply-to inbox (includes `session_id` for follow-ups)
7. Session persists in orchestrator memory — follow-up messages with same `session_id` get full context

### Containers

| Container | Language | Responsibility | Has access to |
|-----------|----------|----------------|---------------|
| **orchestrator** | Raku | Decomposes tasks, manages sessions+history, JetStream admin, synthesizes, streams | NATS only |
| **spawner** | Raku | Manages worker containers via Docker REST API | Docker socket + NATS |
| **worker** | Raku | Ephemeral pull consumer, processes tasks with tools, streams progress, auto-terminates | NATS only |
| **model-deepseek** | Raku | Calls DeepSeek API, decides tool calls | API key + NATS |
| **tool-executor** | Raku | Executes shell, reads/writes files | Sandbox + NATS |
| **nats** | Go | Message broker with JetStream | Internal network |

### Isolation

- API key **never leaves** the `model-deepseek` container
- Shell and filesystem **only in** `tool-executor`
- Docker socket **only in** `spawner` — workers/orchestrator can't touch Docker
- Orchestrator and workers **have no** shell, key, or socket — they only route NATS messages
- Model **does not execute** anything — it only decides tool calls
- Each worker has **isolated conversation context**
- Sessions live in orchestrator memory (lost on restart — PoC #6 will persist)

## PoCs

| PoC | Description | Status |
|-----|-------------|--------|
| PoC #1 | Agent ↔ Model via NATS (simple pub/sub) | ✅ |
| PoC #2 | Multi-turn tool calling with isolated sandbox | ✅ |
| PoC #3 | Multi-agent delegation — decomposition + fixed workers | ✅ |
| PoC #4 | Worker pool with JetStream + auto-scaling (spawner) | ✅ |
| PoC #5 | Streaming progress + session context + conversation history | ✅ |
| PoC #6 | Persistent sessions, worker GC, health checks | 🔜 |

## PoC #6 — Persistence + Resilience

**Goal**: survive restarts and clean up after yourself.

1. **Persistent sessions**: save/load sessions to JetStream KV or files, survive orchestrator restart
2. **Worker GC**: spawner periodically prunes idle/dead containers
3. **Health checks**: NATS health endpoints, auto-restart failed containers
4. **WebSocket bridge** (if time): real-time progress to browser

## Structure

```
camelia/
├── containers/
│   ├── orchestrator/    # Raku — decomposition + synthesis + JetStream admin + sessions
│   ├── spawner/         # Raku — dynamic worker lifecycle via Docker API
│   ├── worker/          # Raku — ephemeral pull consumer + progress streaming
│   ├── agent/           # Raku — single-agent (PoC #2, legacy)
│   ├── model-deepseek/  # Raku — LLM provider
│   ├── tool-executor/   # Raku — execution sandbox
│   └── base/            # Raku base image
├── scripts/
│   └── test-session.sh  # PoC #5 test: subscribe to streams + publish task
├── docker-compose.yaml
├── .env.example
└── README.md
```

## Running

```bash
# 1. Set API key
cp .env.example .env && vim .env

# 2. Build + start
docker compose build && docker compose up -d

# 3. Quick test (streaming progress visible in real-time!)
./scripts/test-session.sh

# Or manually:
nats sub "session.sess-abc.stream" &      # orchestrator milestones
nats sub "session.sess-abc.worker.>.progress" &  # worker thinking
nats sub inbox &                          # final response
nats pub orchestrator.task '{"prompt":"...","session_id":"sess-abc"}' --reply inbox
```

## NATS Subjects

| Subject | Direction | Description |
|---------|-----------|-------------|
| `orchestrator.task` | caller → orchestrator | User prompt with `session_id` + reply-to inbox |
| `worker.task.*` | orchestrator → JetStream | Tasks published to stream WORKER_TASKS |
| `spawner.control` | orchestrator → spawner | Worker lifecycle (ensure/status/stop_all) |
| `model.deepseek.completion` | worker/orch → model | Prompt + history + tools |
| `tools.exec.{name}` | worker → executor | Tool call |
| `session.<id>.stream` | orchestrator → caller | **NEW**: real-time milestones (decomposed, synthesizing, done) |
| `session.<id>.worker.<id>.progress` | worker → caller | **NEW**: per-turn progress (thinking, tool_call, finished) |
| `_INBOX.*` | all → all | Dynamic inbox replies |

## License

MIT
