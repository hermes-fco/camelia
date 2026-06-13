# 🌺 Camélia

**Multi-agent AI framework** em Raku com isolamento de credenciais via NATS.
**Toda comunicação entre componentes é feita exclusivamente via NATS.**

## Arquitetura (PoC #3 — Multi-Agent Delegation)

```
   user / CLI
       │
       │ nats pub orchestrator.task '{"prompt":"..."}' --reply inbox
       ▼
┌─────────────────┐
│  ORCHESTRATOR   │  subscribe: orchestrator.task
│  (decompõe +    │
│   sintetiza)    │
└───┬─────────┬───┘
    │         │
    │         │ worker.*.task (paralelo)
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

### Fluxo

1. Usuário publica `orchestrator.task` com `{"prompt": "..."}` via NATS
2. **Orchestrator** recebe, chama model pra **decompor** em 2-3 subtasks paralelas
3. **Spawna workers** publicando `worker.<id>.task` (paralelo, cada um com inbox)
4. Cada **worker** processa sua task (model loop + tools) e responde ao inbox
5. Orquestrador coleta todos, chama model pra **sintetizar** resposta final
6. Resultado enviado ao **inbox reply-to** do caller

### Containers

| Container | Linguagem | Responsabilidade | Tem acesso a |
|-----------|-----------|------------------|--------------|
| **orchestrator** | Raku | Decompõe tarefas, spawna workers, sintetiza | NATS only |
| **worker** | Raku | Agente de longo prazo, processa tasks com tools | NATS only |
| **model-deepseek** | Raku | Chama API DeepSeek, decide tool calls | API key + NATS |
| **tool-executor** | Raku | Executa shell, lê/escreve arquivos | Sandbox + NATS |
| **nats** | Go | Message broker com JetStream | Rede interna |

### Isolamento

- API key **nunca sai** do container `model-deepseek`
- Shell e filesystem **só no** `tool-executor`
- Orquestrador e workers **não têm** shell nem key — só roteiam mensagens NATS
- Model **não executa** nada — só decide tool calls
- Cada worker tem **contexto de conversa isolado**

## PoCs

| PoC | Descrição | Status |
|-----|-----------|--------|
| PoC #1 | Agent ↔ Model via NATS (pub/sub simples) | ✅ |
| PoC #2 | Multi-turn tool calling com sandbox isolado | ✅ |
| PoC #3 | Multi-agent delegation — decomposição + workers paralelos | ✅ |
| PoC #4 | Registry, auto-pause/unpause, streaming | 🔜 |

## Estrutura

```
camelia/
├── containers/
│   ├── orchestrator/    # Raku — decomposição + síntese
│   │   ├── Dockerfile
│   │   └── orchestrator.raku
│   ├── worker/          # Raku — agente long-running
│   │   ├── Dockerfile
│   │   └── worker.raku
│   ├── agent/           # Raku — single-agent (PoC #2, legacy)
│   │   ├── Dockerfile
│   │   └── agent.raku
│   ├── model-deepseek/  # Raku — provider LLM
│   │   ├── Dockerfile
│   │   └── service.raku
│   ├── tool-executor/   # Raku — sandbox de execução
│   │   ├── Dockerfile
│   │   └── service.raku
│   └── base/            # Raku base image
│       └── Dockerfile
├── lib/
│   ├── Camelia/         # Módulos Raku compartilhados
│   └── nats.raku/       # Fork do nats.raku
├── docker-compose.yaml
├── .env.example
└── README.md
```

## Rodando

```bash
# 1. Configure a chave da API
cp .env.example .env
# Edite .env com sua DEEPSEEK_API_KEY

# 2. Suba o stack (tudo long-running, aguardando mensagens NATS)
docker compose up -d

# 3. Envie um prompt via NATS CLI
nats pub orchestrator.task '{"prompt":"Analise /root/camelia: descreva containers/ e lib/"}' --reply inbox

# Ou com um subscriber esperando a resposta:
nats sub inbox     # em outro terminal, antes de publicar
nats pub orchestrator.task '{"prompt":"..."}' --reply inbox
```

## Subjects NATS

| Subject | Direção | Descrição |
|---------|---------|-----------|
| `orchestrator.task` | caller → orchestrator | Prompt do usuário (com reply-to inbox) |
| `worker.{id}.task` | orchestrator → worker | Subtask delegada (use `_` not `-` in ID) |
| `model.deepseek.completion` | worker/orch → model | Prompt + histórico + tools |
| `tools.exec.{name}` | worker → executor | Tool call |
| `_INBOX.*` | todos → todos | Respostas via inbox dinâmico |

## Licença

MIT
