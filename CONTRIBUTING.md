# Contributing to Conduit

Thanks for contributing. Adapter PRs especially welcome.

## Adding a New Adapter

1. Create `datafly/adapters/your_db.py`
2. Subclass `BaseAdapter` and implement all four methods:
   - `connect()`
   - `introspect_schema()` → return normalized schema dict
   - `get_query_history(limit)` → return list of query dicts
   - `execute(query, params)` → return list of row dicts
3. Register in `AdapterFactory` in `datafly/gateway.py`
4. Add tests in `tests/adapters/test_your_db.py`
5. Update the adapter table in README.md

## Schema Normalization Contract

All adapters must return schema in this format:

```python
{
    "adapter": str,           # adapter name
    "adapter_type": str,      # e.g. "postgres", "mongo"
    "tables": {
        "table_name": {
            "columns": [{"name": str, "type": str, "nullable": bool}],
            "row_count_estimate": int,
            "primary_key": str | None,
            "foreign_keys": [{"column": str, "references": str}]
        }
    },
    "views": { ... }          # same structure
}
```

This is what the Context Agent receives. Richer metadata = better context.

## Running Tests

```bash
pip install -e ".[dev]"
pytest tests/
```

## Code Style

```bash
ruff check .
black .
```
