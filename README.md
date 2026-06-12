# 🌺 Camélia

**Multi-agent AI framework** em Raku e Python com isolamento de credenciais via NATS.

## Arquitetura

```
┌──────────┐  prompt + tools   ┌─────────────────┐
│  AGENT   │ ────────────────▶ │ MODEL-DEEPSEEK  │
│ (router) │                   │  (API key)      │
│          │ ◀──────────────── │                 │
│          │  resposta/texto   └─────────────────┘
│          │
│          │  tool_call        ┌─────────────────┐
│          │ ────────────────▶ │ TOOL-EXECUTOR   │
│          │                   │ (sandbox shell) │
│          │ ◀──────────────── │                 │
│          │  tool_result      └─────────────────┘
└──────────┘
         │        NATS        │
         └────────────────────┘
```

### Containers

| Container | Linguagem | Responsabilidade | Tem acesso a |
|-----------|-----------|------------------|--------------|
| **agent** | Raku | Orquestra conversa, roteia tool calls | NATS only |
| **model-deepseek** | Python | Chama API DeepSeek, decide tools | API key + NATS |
| **tool-executor** | Python | Executa shell, lê/escreve arquivos | Sandbox + NATS |
| **nats** | Go | Message broker | Rede interna |

### Isolamento

- API key **nunca sai** do container `model-deepseek`
- Shell e filesystem **só no** `tool-executor`
- Agent **não tem** shell nem key — só roteia mensagens NATS
- Model **não executa** nada — só decide tool calls

## PoCs

| PoC | Descrição | Status |
|-----|-----------|--------|
| PoC #1 | Agent ↔ Model via NATS (pub/sub simples) | ✅ |
| PoC #2 | Multi-turn tool calling com sandbox isolado | ✅ |
| PoC #3 | Multi-agente, streaming, registry | 🔜 |

## Estrutura

```
camelia/
├── containers/
│   ├── agent/           # Raku — orquestrador
│   │   ├── Dockerfile
│   │   └── agent.raku
│   ├── model-deepseek/  # Python — provider LLM
│   │   ├── Dockerfile
│   │   └── service.py
│   ├── tool-executor/   # Python — sandbox de execução
│   │   ├── Dockerfile
│   │   └── service.py
│   └── base/            # Raku base image (legacy)
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

# 2. Suba o stack
docker compose up -d

# 3. Rode o agente
docker compose run --rm agent

# Ou customize o prompt:
docker compose run --rm -e PROMPT="Crie um script em Python que soma 2 números" agent
```

## Subjects NATS

| Subject | Direção | Descrição |
|---------|---------|-----------|
| `model.deepseek.completion` | agent → model | Prompt + histórico + tools |
| `_INBOX.model.*` | model → agent | Resposta (inbox dinâmico) |
| `tools.exec.{name}` | agent → executor | Tool call |
| `_INBOX.tool.*` | executor → agent | Resultado (inbox dinâmico) |

## Licença

MIT
