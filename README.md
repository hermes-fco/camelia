# 🌺 Camélia

**Multi-agent AI framework** in Raku with credential isolation via NATS.
**All component communication is exclusively via NATS.**

## Architecture (PoC #3 — Multi-Agent Delegation)

```
   user / CLI
       │
       │ nats pub orchestrator.task '{"prompt":"..."}' --reply inbox
       ▼
┌─────────────────┐
│  ORCHESTRATOR   │  subscribe: orchestrator.task
│  (decomposes +  │
│   synthesizes)  │
└───┬─────────┬───┘
    │         │
    │         │ worker.*.task (parallel)
    ▼         ▼
┌────────────┐  ┌────────────┐
│  WORKER A  │  │  WORKER B  │  subscribe: worker.<id>.task
│ (code-     │  │ (doc-      │
│  reader)   │  │  writer)   │
└──┬─────┬───┘  └──┬─────┬───┘
   │     │         │     │
   │     │         │     │
   ▼     ▼         ▼     ▼
┌──────────┐     ┌──────────────┐
│  MODEL   │     │    TOOL      │
│ DEEPSEEK │     │  EXECUTOR    │
│ (API key)│     │  (sandbox)   │
└──────────┘     └──────────────┘
      │                │
      └────── NATS ────┘
```

### Flow

1. User publishes `orchestrator.task` with `{"prompt": "..."}` via NATS
2. **Orchestrator** receives it, calls model to **decompose** into 2-3 parallel subtasks
3. **Spawns workers** by publishing `worker.<id>.task` (parallel, each with inbox reply)
4. Each **worker** processes its task (model loop + tools) and replies via inbox
5. Orchestrator collects all results, calls model to **synthesize** final response
6. Result sent to caller's **inbox reply-to**

### Containers

| Container | Language | Responsibility | Has access to |
|-----------|----------|----------------|---------------|
| **orchestrator** | Raku | Decomposes tasks, spawns workers, synthesizes | NATS only |
| **worker** | Raku | Long-running agent, processes tasks with tools | NATS only |
| **model-deepseek** | Raku | Calls DeepSeek API, decides tool calls | API key + NATS |
| **tool-executor** | Raku | Executes shell, reads/writes files | Sandbox + NATS |
| **nats** | Go | Message broker with JetStream | Internal network |

### Isolation

- API key **never leaves** the `model-deepseek` container
- Shell and filesystem **only in** `tool-executor`
- Orchestrator and workers **have no** shell or key — they only route NATS messages
- Model **does not execute** anything — it only decides tool calls
- Each worker has **isolated conversation context**

## PoCs

| PoC | Description | Status |
|-----|-------------|--------|
| PoC #1 | Agent ↔ Model via NATS (simple pub/sub) | ✅ |
| PoC #2 | Multi-turn tool calling with isolated sandbox | ✅ |
| PoC #3 | Multi-agent delegation — decomposition + parallel workers | ✅ |
| PoC #4 | Registry, auto-pause/unpause, streaming | 🔜 |

## Structure

```
camelia/
├── containers/
│   ├── orchestrator/    # Raku — decomposition + synthesis
│   │   ├── Dockerfile
│   │   └── orchestrator.raku
│   ├── worker/          # Raku — long-running agent
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

# 2. Start the stack (all long-running, waiting for NATS messages)
docker compose up -d

# 3. Send a prompt via NATS CLI
nats pub orchestrator.task '{"prompt":"Analyze /root/camelia: describe containers/ and lib/"}' --reply inbox

# Or with a subscriber waiting for the response:
nats sub inbox     # in another terminal, before publishing
nats pub orchestrator.task '{"prompt":"..."}' --reply inbox
```

## NATS Subjects

| Subject | Direction | Description |
|---------|-----------|-------------|
| `orchestrator.task` | caller → orchestrator | User prompt (with reply-to inbox) |
| `worker.{id}.task` | orchestrator → worker | Delegated subtask (hyphens OK after grammar fix) |
| `model.deepseek.completion` | worker/orch → model | Prompt + history + tools |
| `tools.exec.{name}` | worker → executor | Tool call |
| `_INBOX.*` | all → all | Dynamic inbox replies |

## License

MIT
