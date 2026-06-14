# PoC #3 — Typed Workers + Factory

## Goal
Specialize workers by type (shell, factory) with subject-based NATS routing.
The factory worker creates new worker types from prompts, using the model
to generate code, `raku -c` for validation, and session-store for build memory.

## Architecture

```
orchestrator
    │
    ├── worker.shell.task.<id>  → worker.shell (executes tools)
    └── worker.factory.request  → worker.factory (creates new worker types)
                                       │
                                       ├── model.deepseek.completion
                                       ├── session.store.* (build memory)
                                       └── raku -c (validation)
```

## Files Changed

### New Files
- `containers/worker-shell/worker-shell.raku` — Shell worker (direct NATS, request-reply)
- `containers/worker-shell/Dockerfile` — Builds on camelia-base
- `containers/worker-factory/worker-factory.raku` — Factory worker
- `containers/worker-factory/Dockerfile` — Builds on camelia-base
- `templates/worker-api.raku` — Template for API worker generation

### Modified Files
- `containers/orchestrator/orchestrator.raku` — Add worker_type to decomposition, route tasks
- `containers/spawner/spawner.raku` — Accept WORKER_TYPE, build typed images
- `docker-compose.yaml` — Add worker-shell, worker-factory services
- `tests/camelia-integration.raku` — Add typed worker tests

### Archived (not deleted)
- `containers/worker/worker.raku` — Generic worker kept for backward compat

## Implementation Order (TDD)

1. **Test**: Write integration tests for typed workers + factory
2. **worker-shell**: Extract shell execution, subscribe to `worker.shell.>`
3. **worker-factory**: Template-driven generation with model + validation
4. **orchestrator update**: Route tasks to typed workers
5. **spawner update**: Build typed worker images
6. **End-to-end test**: Full pipeline with typed workers

## Key Design Decisions

- Workers use direct NATS subscriptions (not JetStream) for PoC simplicity
- Factory uses session-store for build memory (idempotency, recovery)
- Model communication via NATS (never direct)
- Template boilerplate injected, model only generates tool-specific logic
- Validation: `raku -c` → fail → error to model → retry (max 3)
- Generated workers saved to `/workers/<type>/` for persistence
