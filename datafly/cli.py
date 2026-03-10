"""
Datafly CLI — connect, build, query from the terminal.

Usage:
  datafly status
  datafly connect postgres://user:pass@localhost/mydb --name prod
  datafly build [--force]
  datafly query "What was revenue last quarter?"
  datafly review
  datafly approve revenue
  datafly serve [--port 8000]
  datafly serve-mcp [--port 8080]
"""

import sys
import json
import argparse
import logging


def main():
    parser = argparse.ArgumentParser(
        prog="datafly",
        description="Datafly — Universal Data Gateway"
    )
    sub = parser.add_subparsers(dest="command")

    # status
    sub.add_parser("status", help="Show connected adapters and context state")

    # connect
    p_connect = sub.add_parser("connect", help="Connect a data source")
    p_connect.add_argument("connection_string", help="e.g. postgres://user:pass@host/db")
    p_connect.add_argument("--name", required=True, help="Alias for this adapter")

    # build
    p_build = sub.add_parser("build", help="Build the semantic context layer")
    p_build.add_argument("--force", action="store_true", help="Rebuild even if context exists")

    # query
    p_query = sub.add_parser("query", help="Run a natural language query")
    p_query.add_argument("question", nargs="+", help="Natural language question")
    p_query.add_argument("--adapter", default=None, help="Force a specific adapter")
    p_query.add_argument("--json", action="store_true", help="Output raw JSON")

    # review
    sub.add_parser("review", help="List context items flagged for human review")

    # approve
    p_approve = sub.add_parser("approve", help="Approve a reviewed context item")
    p_approve.add_argument("entity", help="Entity or metric name to approve")

    # tribal
    p_tribal = sub.add_parser("tribal", help="Add a business rule to the context")
    p_tribal.add_argument("rule", nargs="+", help="Business rule in plain English")

    # serve
    p_serve = sub.add_parser("serve", help="Start the REST API server")
    p_serve.add_argument("--port", type=int, default=8000)
    p_serve.add_argument("--host", default="0.0.0.0")

    # serve-mcp
    p_mcp = sub.add_parser("serve-mcp", help="Start the MCP server")
    p_mcp.add_argument("--port", type=int, default=8080)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    # Load config
    try:
        from datafly.config import DataflyConfig
        config = DataflyConfig.from_env()
    except ValueError as e:
        print(f"❌ Config error: {e}")
        sys.exit(1)

    from datafly.gateway import Datafly
    df = Datafly(
        context_yaml_path=config.context_yaml_path,
        context_db_url=config.context_db_url,
        context_backend=config.context_backend
    )

    # ── Commands ──────────────────────────────────────────────────────────

    if args.command == "status":
        status = df.status()
        print("\n📡 Datafly Status")
        print(f"  Context backend : {df.context_store.backend}")
        print(f"  Context exists  : {status['context']['exists']}")
        print(f"  Context version : {status['context']['version']}")
        print(f"  Review queue    : {status['context']['review_items']} item(s)")
        print(f"  Adapters        : {len(status['adapters'])} connected")
        for name, info in status["adapters"].items():
            print(f"    • {name} ({info['type']})")

    elif args.command == "connect":
        print(f"Connecting {args.name}...")
        df.connect(args.connection_string, args.name)
        print(f"✅ Connected: {args.name}")

    elif args.command == "build":
        print("🔨 Building context layer...")
        ctx = df.build_context(force_rebuild=args.force)
        print(f"✅ Context built (v{ctx.get('_meta',{}).get('version',1)})")
        print(f"   Entities  : {len(ctx.get('entities', {}))}")
        print(f"   Metrics   : {len(ctx.get('metrics', {}))}")
        print(f"   Rules     : {len(ctx.get('routing_rules', []))}")
        review = ctx.get("review_required", [])
        if review:
            print(f"   ⚠️  Review  : {', '.join(review)}")

    elif args.command == "query":
        question = " ".join(args.question)
        print(f"🔍 {question}")
        result = df.query(question, adapter_hint=args.adapter)
        if args.json:
            print(json.dumps(result, indent=2, default=str))
        elif result["success"]:
            print(f"✅ [{result['adapter']}] {result['row_count']} row(s)")
            if result.get("query"):
                print(f"   Query : {result['query'][:120]}...")
            if result.get("context_applied"):
                print(f"   Context applied: {', '.join(result['context_applied'])}")
            rows = result.get("rows", [])
            if rows:
                _print_table(rows[:10])
        else:
            print(f"❌ Error: {result['error']}")
            if result.get("query"):
                print(f"   Query attempted: {result['query'][:120]}")

    elif args.command == "review":
        items = df.context_store.get_review_items()
        if not items:
            print("✅ No items need review")
        else:
            print(f"⚠️  {len(items)} item(s) need human review:")
            ctx = df.context_store.load()
            for item in items:
                defn = ctx.get("entities", {}).get(item) or ctx.get("metrics", {}).get(item, {})
                conf = defn.get("confidence", "?")
                desc = defn.get("description", "no description")
                print(f"  • {item} (confidence: {conf})")
                print(f"    {desc}")
                print(f"    Run: datafly approve {item}")

    elif args.command == "approve":
        df.context_store.approve(args.entity)
        print(f"✅ Approved: {args.entity}")

    elif args.command == "tribal":
        rule = " ".join(args.rule)
        df.context_store.add_tribal_knowledge(rule)
        print(f"✅ Rule added: {rule}")

    elif args.command == "serve":
        print(f"🚀 Datafly API starting on http://{args.host}:{args.port}")
        print(f"   Docs: http://{args.host}:{args.port}/docs")
        df.serve(host=args.host, port=args.port)

    elif args.command == "serve-mcp":
        print(f"🚀 Datafly MCP server starting on port {args.port}")
        df.serve_mcp(port=args.port)


def _print_table(rows: list[dict]) -> None:
    if not rows:
        return
    cols = list(rows[0].keys())[:6]  # cap at 6 columns
    widths = {c: max(len(str(c)), max(len(str(r.get(c, ""))) for r in rows)) for c in cols}
    widths = {c: min(w, 30) for c, w in widths.items()}  # cap width
    header = "  " + "  ".join(str(c).ljust(widths[c]) for c in cols)
    sep = "  " + "  ".join("-" * widths[c] for c in cols)
    print(header)
    print(sep)
    for row in rows:
        print("  " + "  ".join(str(row.get(c, ""))[:widths[c]].ljust(widths[c]) for c in cols))


if __name__ == "__main__":
    main()
