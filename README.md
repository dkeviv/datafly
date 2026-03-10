# Datafly

**A universal data gateway with an AI-powered semantic context layer for data agents.**

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://python.org)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

---

Data agents fail not because models are bad at SQL — but because they have no idea how your business actually works.

**Conduit solves this.** It sits between your agents and your databases, automatically building a semantic context layer from your existing schemas and query history. Agents connect to one endpoint. They get answers that actually make sense.

```
Your Agents
    ↓
[ Datafly Gateway ]   ← single connection point, any database
    ↓
[ Context Layer ]     ← auto-built from your schema + query history
    ↓
Postgres · Snowflake · BigQuery · MongoDB · Salesforce · ...
```

---

## The Problem

```sql
-- Agent asks: "What was revenue last quarter?"
-- Agent generates:
SELECT SUM(amount) FROM orders WHERE date > '2024-10-01'

-- Reality: revenue lives in fct_revenue, fiscal Q4 ends Nov 30,
-- and you exclude refunds. Agent has no idea. Query is wrong.
```

Without context, agents hallucinate metrics, query the wrong tables, and produce numbers nobody trusts.

## The Solution

Conduit's **Context Creation Agent** introspects your databases and mines your query history to automatically build a semantic model — entities, metric definitions, source-of-truth routing, business rules — then serves it to your agents at query time.

```yaml
# Auto-generated context (datafly/context/metrics.yaml)
metrics:
  revenue:
    description: "Recognized ARR, excluding refunds and trials"
    source_of_truth: fct_revenue
    fiscal_quarter: "Nov 30 end"
    confidence: 0.94
    derived_from: "query_history_analysis + schema_introspection"
```

---

## Features

- **Universal adapters** — Postgres, Snowflake, BigQuery, Redshift, MongoDB, DynamoDB, Salesforce, HubSpot (more via PRs)
- **Auto context generation** — LLM analyzes schema + query history, outputs a structured semantic model
- **Confidence scoring** — high-confidence definitions auto-accepted, low-confidence flagged for human review
- **MCP compatible** — expose Conduit as an MCP server; your agents connect with zero changes
- **Self-healing** — failed or corrected queries feed back into the context layer automatically
- **Git-native** — context layer is versioned YAML, lives in your repo, reviewed like code
- **Zero lock-in** — Apache 2.0, runs anywhere, own your data

---

## Quickstart

```bash
pip install datafly-gateway
```

```python
from datafly import Datafly

c = Datafly()

# Connect your databases
c.connect("postgres://user:pass@localhost/mydb", name="prod_postgres")
c.connect("snowflake://account/warehouse/db", name="analytics")

# Auto-build context layer from schema + query history
c.build_context()

# Query through unified gateway — context injected automatically
result = c.query("What was revenue last quarter?")
```

That's it. Conduit introspects your databases, mines query history, builds the semantic model, and serves it at query time.

---

## Architecture

```
┌─────────────────────────────────────────────┐
│              Datafly Gateway                │
│                                             │
│  ┌──────────┐    ┌────────────────────────┐ │
│  │  REST /  │    │   Context Creation     │ │
│  │  MCP API │───▶│       Agent            │ │
│  └──────────┘    │  (LLM-powered)         │ │
│       │          └────────────┬───────────┘ │
│       │                       │             │
│       ▼          ┌────────────▼───────────┐ │
│  ┌──────────┐    │   Semantic Context     │ │
│  │  Query   │◀───│       Layer            │ │
│  │  Router  │    │  (versioned YAML/JSON) │ │
│  └────┬─────┘    └────────────────────────┘ │
│       │                                     │
└───────┼─────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────┐
│              Adapter Layer                │
│  Postgres  Snowflake  MongoDB  Salesforce │
│  BigQuery  Redshift   DynamoDB  HubSpot   │
└───────────────────────────────────────────┘
```

### Components

| Component | Description |
|---|---|
| **Gateway** | FastAPI core, handles routing, auth, rate limiting |
| **Adapters** | Thin DB-specific wrappers — introspect schema, execute queries |
| **Context Agent** | LLM chain that builds semantic model from schema + query history |
| **Context Layer** | Versioned YAML — entities, metrics, routing rules, confidence scores |
| **Query Router** | Matches incoming queries to right adapter + injects context |
| **MCP Server** | Exposes gateway as MCP endpoint for agent frameworks |

---

## Context Layer Example

After running `c.build_context()`, Conduit generates a human-reviewable context file:

```yaml
# datafly/context/context.yaml

entities:
  customer:
    source_of_truth: dim_customer
    aliases: ["client", "account", "user"]
    primary_key: customer_id
    confidence: 0.97

  revenue:
    source_of_truth: fct_revenue
    aliases: ["ARR", "MRR", "bookings"]
    exclude_fields: ["refund_amount", "trial_revenue"]
    fiscal_year_end: "November 30"
    confidence: 0.91
    review_flag: false

routing_rules:
  - pattern: "revenue*"
    adapter: snowflake_analytics
    table: fct_revenue
  - pattern: "customer*"
    adapter: prod_postgres
    table: dim_customer

tribal_knowledge:
  - "For CRM data, use Salesforce for deals after 2024-01-01, legacy Postgres before"
  - "Monthly active users excludes internal @company.com accounts"
```

Low-confidence entries are flagged automatically. Edit and commit — it's just a file.

---

## Supported Adapters

| Adapter | Status | Introspection | Query History |
|---|---|---|---|
| PostgreSQL | ✅ Stable | `information_schema` | `pg_stat_statements` |
| Snowflake | ✅ Stable | `INFORMATION_SCHEMA` | `QUERY_HISTORY` |
| BigQuery | ✅ Stable | `INFORMATION_SCHEMA` | `JOBS` table |
| MongoDB | ✅ Stable | Document sampling | `system.profile` |
| Salesforce | ✅ Stable | `describe()` API | API logs |
| Redshift | ✅ Stable | `information_schema` | `STL_QUERY` |
| DynamoDB | ✅ Stable | Table + GSI describe | CloudWatch (optional) |
| HubSpot | ✅ Stable | Properties API | — |

---

## MCP Integration

Conduit exposes itself as an MCP server out of the box:

```python
c.serve_mcp(port=8080)
# Agents connect to: mcp://localhost:8080
```

Works with Claude, LangChain, LlamaIndex, CrewAI, and any MCP-compatible agent framework.

---

## Roadmap

- [x] Core gateway + adapter pattern
- [x] Postgres, Snowflake, BigQuery, MongoDB, Salesforce adapters
- [x] LLM-powered context creation agent
- [x] Confidence scoring + human review flags
- [x] MCP server mode
- [ ] Curation UI (cloud)
- [ ] Self-healing feedback loop
- [ ] dbt / LookML import for existing semantic models
- [ ] Redshift, DynamoDB, HubSpot adapters
- [ ] Multi-tenant support (cloud)
- [ ] RBAC + audit logs (enterprise)

---

## Contributing

Conduit is built in the open. Adapter contributions especially welcome — if your database isn't listed, [open a PR](CONTRIBUTING.md).

```bash
git clone https://github.com/cogumi-ai/datafly
cd datafly
pip install -e ".[dev]"
pytest tests/
```

---

## License

Apache 2.0 — free to use, modify, and distribute. Commercial cloud and enterprise offerings available at [datafly.dev](https://datafly.dev).

---

*Built by [Cogumi](https://cogumi.ai) · [Discord](https://discord.gg/datafly) · [Docs](https://docs.datafly.dev)*
