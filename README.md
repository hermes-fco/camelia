# 🌺 Camélia

**Multi-agent AI framework** in Raku with credential isolation via NATS.
**All component communication is exclusively via NATS.**

## Architecture (PoC #4 — Worker Pool with JetStream + Auto-Scaling)

```
   user / CLI
       │
       │ nats pub orchestrator.task '{"prompt":"..."}' --reply inbox
       ▼
┌─────────────────┐     ┌──────────┐
│  ORCHESTRATOR   │────▶│  SPAWNER │  subscribe: orchestrator.task
│  (decomposes +  │     │ (docker  │  spawner.control
│   synthesizes)  │     │  socket) │
└───┬─────────────┘     └────┬─────┘
    │                        │
    │ worker.task.*          │ spawn dynamic workers
    │ (JetStream)            ▼
    │              ┌──────────────────┐
    │              │ WORKER 1..N      │  pull consumer
    │              │ (ephemeral,      │  JetStream WORKER_TASKS
    │              │  idle timeout    │
    │              │  5 min)          │
    │              └──┬─────┬─────────┘
    │                 │     │
    │                 │     │
    ▼                 ▼     ▼
┌──────────┐     ┌──────────────┐
│  MODEL   │     │    TOOL      │
│ DEEPSEEK │     │  EXECUTOR    │
│ (API key)│     │  (sandbox)   │
└──────────┘     └──────────────┘
     │                  │
     └────── NATS ──────┘
```

### Flow

1. User publishes `orchestrator.task` with `{"prompt": "..."}` via NATS
2. **Orchestrator** receives it, calls model to **decompose** into subtasks
3. Publishes tasks to JetStream **stream** `WORKER_TASKS` (subject `worker.task.*`)
4. Asks **spawner** (`spawner.control`) to ensure N workers are running
5. **Spawner** creates Docker containers dynamically via REST API (`/var/run/docker.sock`)
6. **Workers** pull tasks from JetStream consumer (queue group, one task per worker)
7. Each worker processes its task (model loop + tools) and replies via inbox
8. Orchestrator collects all results, calls model to **synthesize** final response
9. Worker idles for 5 minutes, then **self-terminates** (dangling container cleanup TBD)
10. Result sent to caller's **inbox reply-to**

### Containers

| Container | Language | Responsibility | Has access to |
|-----------|----------|----------------|---------------|
| **orchestrator** | Raku | Decomposes tasks, creates JetStream stream, spawns workers, synthesizes | NATS only |
| **spawner** | Raku | Manages worker containers via Docker REST API | Docker socket + NATS |
| **worker** | Raku | Ephemeral pull consumer, processes tasks with tools, auto-terminates | NATS only |
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

## PoCs

| PoC | Description | Status |
|-----|-------------|--------|
| PoC #1 | Agent ↔ Model via NATS (simple pub/sub) | ✅ |
| PoC #2 | Multi-turn tool calling with isolated sandbox | ✅ |
| PoC #3 | Multi-agent delegation — decomposition + fixed workers | ✅ |
| PoC #4 | Worker pool with JetStream + auto-scaling (spawner) | ✅ |
| PoC #5 | Streaming results + session context | 🔜 |

## PoC #5 — Streaming Results + Session Context

**Goal**: Don't make the caller wait until everything is done.

1. Workers send **partial progress** as they work (not just final result)
2. Orchestrator **streams** intermediate results to caller's inbox
3. **Session context**: multiple sequential prompts share worker pool + conversation history
4. **WebSocket bridge**: real-time progress to a web UI or CLI

## Structure

```
camelia/
├── containers/
│   ├── orchestrator/    # Raku — decomposition + synthesis + JetStream admin
│   │   ├── Dockerfile
│   │   └── orchestrator.raku
│   ├── spawner/         # Raku — dynamic worker lifecycle via Docker API
│   │   ├── Dockerfile
│   │   └── spawner.raku
│   ├── worker/          # Raku — ephemeral pull consumer
│   │   ├── Dockerfile
│   │   └── worker.raku
│   ├── agent/           # Raku — single-agent (PoC #2, legacy)
│   │   ├── Dockerfile
│   │   └── agent.raku
│   ├── model-deepseek/  # Raku — LLM provider
│   │   ├── Dockerfile
│   │   └── service.raku
│   ├── tool-executor/   # Raku — execution sandbox
│   │   ├── Dockerfile
│   │   └── service.raku
│   └── base/            # Raku base image
│       └── Dockerfile
├── lib/
│   ├── Camelia/         # Shared Raku modules
│   └── nats.raku/       # nats.raku fork
├── docker-compose.yaml
├── .env.example
└── README.md
```

## Running

```bash
# 1. Set the API key
cp .env.example .env
# Edit .env with your DEEPSEEK_API_KEY

# 2. Build all images
docker compose build

# 3. Start the stack
docker compose up -d

# 4. Send a prompt via NATS CLI
nats pub orchestrator.task '{"prompt":"Analyze /root/camelia: describe containers/ and lib/"}' --reply inbox

# Or with a subscriber waiting for the response:
nats sub inbox     # in another terminal, before publishing
nats pub orchestrator.task '{"prompt":"..."}' --reply inbox
```

## NATS Subjects

| Subject | Direction | Description |
|---------|-----------|-------------|
| `orchestrator.task` | caller → orchestrator | User prompt (with reply-to inbox) |
| `worker.task.*` | orchestrator → JetStream | Tasks published to stream WORKER_TASKS |
| `spawner.control` | orchestrator → spawner | Worker lifecycle (ensure/status/stop_all) |
| `model.deepseek.completion` | worker/orch → model | Prompt + history + tools |
| `tools.exec.{name}` | worker → executor | Tool call |
| `_INBOX.*` | all → all | Dynamic inbox replies |

## License

MIT
