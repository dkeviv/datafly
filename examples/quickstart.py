"""
Datafly — End-to-End Example
=============================

Shows the full flow: connect → build context → query → feedback.
Replace connection strings with your own before running.

pip install datafly
export ANTHROPIC_API_KEY=your_key
python examples/quickstart.py
"""

from datafly import Conduit

# 1. Initialize gateway
c = Datafly(context_path="./my_context/context.yaml")

# 2. Connect your databases (add as many as you have)
c.connect("postgresql://user:pass@localhost:5432/prod_db", name="postgres_prod")
# c.connect("snowflake://user:pass@account/warehouse/database", name="snowflake_analytics")
# c.connect("mongodb://user:pass@localhost:27017/mydb", name="mongo_app")

# 3. Auto-build the context layer
# This introspects schemas + query history and calls the LLM to generate
# a semantic model. Takes ~30-60 seconds for a typical database.
print("Building context layer...")
context = c.build_context()

print(f"\n✅ Context built:")
print(f"   Entities:        {len(context.get('entities', {}))}")
print(f"   Metrics:         {len(context.get('metrics', {}))}")
print(f"   Routing rules:   {len(context.get('routing_rules', []))}")
print(f"   Review required: {context.get('review_required', [])}")

# 4. See what tribal knowledge was inferred
print("\n📖 Inferred business rules:")
for rule in context.get("tribal_knowledge", []):
    print(f"   • {rule}")

# 5. Query through the gateway — context injected automatically
questions = [
    "What was revenue last quarter?",
    "How many active customers do we have?",
    "Show me churn rate by customer segment",
]

print("\n🤖 Running queries through context-aware gateway...")
for q in questions:
    result = c.query(q)
    print(f"\nQ: {q}")
    if result["success"]:
        print(f"   Routed to: {result['adapter']}")
        print(f"   Context applied: {result.get('context_applied', [])}")
        print(f"   SQL: {result.get('sql', '')[:100]}...")
        print(f"   Rows returned: {len(result.get('rows', []))}")
    else:
        print(f"   ❌ Error: {result['error']}")

# 6. Review low-confidence items
print("\n⚠️  Items needing human review:")
for item in c.context_store.get_review_items():
    print(f"   • {item}")

# 7. Add a business rule manually (tribal knowledge)
c.context_store.add_tribal_knowledge(
    "For revenue calculations, always exclude internal test accounts (domain: @test.company.com)"
)

# 8. Approve a reviewed item
# c.context_store.approve("revenue")  # After human review

# 9. Submit feedback to improve context
# c.feedback("query_123", "Revenue should use fct_revenue table, not the orders view")

print("\n✅ Done. Context layer saved to ./my_context/context.yaml")
print("   Edit the YAML directly to add business rules, then commit to Git.")
print("\n   To serve via REST API:")
print("   >>> c.serve(port=8000)")
print("\n   To serve as MCP server:")
print("   >>> c.serve_mcp(port=8080)")
