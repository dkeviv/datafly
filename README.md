# Datafly

**Your data agents are failing because they don't understand your data. Datafly fixes that.**

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://python.org)
[![PyPI](https://img.shields.io/pypi/v/datafly-gateway)](https://pypi.org/project/datafly-gateway)
[![Discord](https://img.shields.io/badge/Discord-join-5865F2)](https://discord.gg/datafly)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

---

## The problem

You connect an agent to your database. You ask it a simple question.

```
You:   "What was our revenue last quarter?"

Agent: SELECT SUM(amount) FROM orders WHERE date > '2024-10-01'
       -> $0.00
```

Wrong table. Wrong date logic. Your fiscal Q4 ends November 30. Revenue lives in `fct_revenue`, not `orders`. Refunds need to be excluded. The agent has no idea — and neither does any tool you've tried.

**This is not a model problem. It's a context problem.**

---

## What Datafly does

Datafly sits between your agents and your databases. It automatically builds a **semantic context layer** — a living model of what your data actually means — and injects it into every query.

```
Agent asks: "What was revenue last quarter?"
                    |
        [ Datafly Context Layer ]
          knows: revenue = fct_revenue
          knows: fiscal Q4 ends Nov 30
          knows: exclude refunds + trials
                    |
SELECT SUM(arr) FROM fct_revenue
WHERE fiscal_quarter = 'FY2025-Q1'
AND is_expansion = FALSE
-> $1,806,000   (correct)
```

No prompt engineering. No manual schema docs. It figures this out from your actual database.

---

## Quickstart

```bash
pip install datafly-gateway[postgres]
```

```python
from datafly import Datafly

df = Datafly()
df.connect("postgresql://user:pass@localhost/mydb", name="prod")
df.build_context()   # LLM reads your schema + query history, builds the model

result = df.query("What was revenue last quarter?")
print(result["rows"])
# [{"fiscal_quarter": "FY2025-Q1", "total_arr": 1806000}]
```

Or with Docker — demo database, UI, everything included:

```bash
git clone https://github.com/dkeviv/datafly
cd datafly
cp .env.example .env   # add your LLM API key
docker compose up
# API: http://localhost:8000
# UI:  open ui/index.html in your browser
```

---

## How the context layer works

When you run `build_context()`, Datafly's Context Agent does three things:

**1. Reads your schema** — every table, column, foreign key, and real sample values (not just types)

**2. Mines your query history** — finds patterns in past SQL to infer business meaning

**3. Produces a semantic model** — entities, metric definitions, routing rules, tribal knowledge, with confidence scores

```yaml
# datafly/context/context.yaml  (auto-generated, human-editable)

metrics:
  revenue:
    description: "Recognized ARR excluding refunds and trial conversions"
    source_of_truth: fct_revenue
    formula: "SUM(arr) WHERE is_expansion = FALSE"
    aliases: ["ARR", "MRR", "bookings"]
    confidence: 0.94

entities:
  customer:
    source_of_truth: dim_customer
    aliases: ["client", "account", "user"]
    primary_key: customer_id
    confidence: 0.97

tribal_knowledge:
  - "Fiscal year ends November 30. Q1=Dec-Feb, Q2=Mar-May, Q3=Jun-Aug, Q4=Sep-Nov"
  - "Active customers: status = 'active' AND churned_at IS NULL"
  - "Enterprise MRR is calculated from fct_revenue, not the CRM"
```

This file lives in your repo. Review it like code. Edit it. Commit it. It gets better with every correction.

---

## The agentic query loop

Datafly doesn't just generate SQL once and hope. It runs a **Plan -> Generate -> Execute -> Reflect -> Retry** loop:

```
Question: "What is our net revenue retention?"
    |
  [Plan]      reason about which tables, joins, and metric definitions apply
    |
  [Generate]  write SQL grounded in the plan + real sample values from the DB
    |
  [Execute]   run against your actual database
    |
  [Reflect]   if error or empty result: reason about what went wrong
    |
  [Retry]     rewrite with the error as context (up to 3 attempts)
    |
  Result      or honest failure with the last attempted query shown
```

No fixed rules. No hardcoded prompt patches. The agent reasons its way to the right answer.

---

## Four ways to use Datafly

Datafly is the same engine underneath — pick the interface that fits your workflow.

```
+--------------------------------------------------+
|             Your choice of interface             |
|                                                  |
|  Web UI      CLI        REST API       MCP       |
|  (humans)  (terminal)  (any client)  (agents)    |
+--------------------------------------------------+
                      |
            [ Datafly Gateway ]
       semantic context + query routing
                      |
      Postgres . Snowflake . Mongo . ...
```

---

### 1 — Web UI

The fastest way to get started. Open `ui/index.html` in any browser — no build step needed beyond the running API.

**Connections tab** — add a database in three steps:

1. Pick the DB type (Postgres, Snowflake, MongoDB, etc.)
2. Paste the connection URI — the format hint updates automatically
3. Click **Test Connection** — if green, click **Connect & Discover Schema**

Datafly tests the connection, introspects the schema, builds the context layer, and drops you into the chat. Done.

**Chat tab** — ask in plain English:

```
You:     "Which enterprise customers haven't logged in this month?"

Result:  2 rows  [Acme Corp: last active 2025-02-14]
                 [Umbrella Ltd: last active 2025-01-30]

SQL:     SELECT c.company_name, MAX(a.event_date) AS last_active
         FROM dim_customer c
         LEFT JOIN fct_user_activity a ON c.customer_id = a.customer_id
         WHERE c.plan = 'enterprise'
         GROUP BY c.company_name
         HAVING MAX(a.event_date) < DATE_TRUNC('month', NOW())
```

Every result shows the generated SQL. If the answer is wrong, type a correction in the feedback box — it updates the context layer immediately.

---

### 2 — CLI

Best for automation, scripts, and developers who live in the terminal.

**Setup:**

```bash
pip install datafly-gateway[postgres]

export ANTHROPIC_API_KEY=sk-ant-...   # or OPENROUTER_API_KEY

datafly connect postgresql://user:pass@localhost/mydb --name prod
datafly build
```

**Query:**

```bash
$ datafly query "What is our MRR by plan?"

Searching: What is our MRR by plan?
[prod] 3 rows

  plan        mrr
  ----------  ----------
  enterprise  139500.00
  growth      10000.00
  starter     1000.00
```

```bash
# Raw JSON for scripting
datafly query "total ARR" --json | jq '.rows[0].total_arr'
```

**Manage context:**

```bash
datafly review                            # show low-confidence items flagged for review
datafly approve revenue                   # approve a flagged metric
datafly tribal "Fiscal year ends Nov 30"  # add a business rule manually
datafly build --force                     # rebuild from scratch
datafly status                            # show connected sources + context state
```

**Full command reference:**

```
datafly status                        Show connected sources and context state
datafly connect <uri> --name <name>   Connect a data source
datafly build [--force]               Build or rebuild the context layer
datafly query "<question>" [--json]   Run a natural language query
datafly review                        List items needing human review
datafly approve <entity>              Approve a flagged entity or metric
datafly tribal "<rule>"               Add a business rule to the context
datafly serve [--port 8000]           Start the REST API server
datafly serve-mcp [--port 8080]       Start the MCP server
```

---

### 3 — REST API

Run Datafly as a persistent service. Any language, any HTTP client.

```bash
datafly serve --port 8000
# Swagger UI: http://localhost:8000/docs
```

**Connect a database:**

```bash
curl -X POST http://localhost:8000/connect \
  -H "Content-Type: application/json" \
  -d '{"connection_string": "postgresql://user:pass@host/db", "name": "prod"}'
```

**Test a connection without saving it:**

```bash
curl -X POST http://localhost:8000/connect/test \
  -H "Content-Type: application/json" \
  -d '{"connection_string": "postgresql://user:pass@host/db", "name": "test"}'

# -> {"status": "ok", "table_count": 12, "tables": ["dim_customer", "fct_revenue", ...]}
```

**Build context:**

```bash
curl -X POST http://localhost:8000/context/build
```

**Query:**

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"question": "What is total ARR by plan?"}'

# -> {"success": true, "query": "SELECT plan, SUM(arr)...", "rows": [...], "row_count": 3}
```

**Submit a correction:**

```bash
curl -X POST http://localhost:8000/feedback \
  -H "Content-Type: application/json" \
  -d '{"query_id": "abc123", "correction": "ARR should exclude trial customers"}'
```

Full endpoint list at `http://localhost:8000/docs`.

---

### 4 — MCP (for AI agents)

Start the MCP server and point any MCP-compatible agent at it. No custom tool definitions needed.

```bash
datafly serve-mcp --port 8080
```

Or from Python:

```python
from datafly import Datafly

df = Datafly()
df.connect("postgresql://...", name="prod")
df.build_context()
df.serve_mcp(port=8080)
```

Three tools are exposed automatically:

| Tool | What it does |
|------|-------------|
| `query_data` | Natural language -> SQL -> results, context injected automatically |
| `get_context` | Returns the semantic model (entities, metrics, tribal knowledge) |
| `list_adapters` | Lists all connected data sources |

**Claude Desktop** (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "datafly": {
      "url": "http://localhost:8080/mcp/sse"
    }
  }
}
```

**LangChain:**

```python
from langchain_mcp import MCPToolkit

toolkit = MCPToolkit(url="http://localhost:8080/mcp/sse")
tools = toolkit.get_tools()
# -> [query_data, get_context, list_adapters]
```

Works with LlamaIndex, CrewAI, AutoGen, and any framework that supports MCP or HTTP tool calls.

---

## Works with every data stack

| Source | Status | Notes |
|--------|--------|-------|
| PostgreSQL | beta | `information_schema` + `pg_stat_statements` |
| Snowflake | beta | `INFORMATION_SCHEMA` + `QUERY_HISTORY` |
| BigQuery | beta | `INFORMATION_SCHEMA` + `JOBS` |
| Redshift | beta | `information_schema` + `STL_QUERY` |
| MongoDB |beta | Document sampling + `system.profile` |
| DynamoDB | beta | Table + GSI describe |
| Salesforce | beta | `describe()` API |
| HubSpot | beta | Properties API |
| MySQL | coming soon | PR welcome |
| dbt / LookML | coming soon | Import existing semantic model |

---

## Architecture

```
Your Agents / LLMs
        |
+---------------------------------------+
|          Datafly Gateway              |
|                                       |
|  Web UI   --+                         |
|  CLI      --+---> Query Router        |
|  REST API --+          |              |
|  MCP      --+          v              |
|                +---------------+      |
|                | Semantic      |      |
|                | Context Layer |      |
|                | (YAML + DB)   |      |
|                +-------+-------+      |
|                        |              |
|                +-------v-------+      |
|                | Context Agent |      |
|                | Plan->Gen     |      |
|                | ->Execute     |      |
|                | ->Reflect     |      |
|                | ->Retry       |      |
|                +---------------+      |
+-------------------+-------------------+
                    |
    Postgres . Snowflake . Mongo . ...
```

---

## Configuration

Copy `.env.example` to `.env` and add one key:

```bash
ANTHROPIC_API_KEY=sk-ant-...
# or
OPENROUTER_API_KEY=sk-or-...   # works with Claude, GPT-4, Gemini
```

Datafly auto-detects which key is present. Everything else has sensible defaults.

---

## Roadmap

- [x] Core gateway with 8 adapters
- [x] LLM-powered context creation agent
- [x] Agentic Plan -> Execute -> Reflect query loop
- [x] Confidence scoring + human review flags
- [x] MCP server
- [x] REST API + Python SDK
- [x] Web UI with connection settings
- [x] Self-correcting feedback loop
- [ ] dbt / LookML import
- [ ] Incremental context updates (no full rebuild needed)
- [ ] Curation UI (cloud)
- [ ] Multi-tenant + RBAC (enterprise)

---

## Contributing

Adapters and semantic model importers are the highest-leverage contributions. See [CONTRIBUTING.md](CONTRIBUTING.md).

```bash
git clone https://github.com/dkeviv/datafly
cd datafly
pip install -e ".[dev]"
pytest tests/   # 28 tests, all should pass
```

---

## License

Apache 2.0. Free to use, modify, deploy. Cloud and enterprise offerings at [datafly.dev](https://datafly.dev).

---

*Built by dkeviv · [Discord](https://discord.gg/datafly) · [datafly.dev](https://datafly.dev)*
