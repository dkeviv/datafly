#!/bin/sh
set -e

echo "Starting Datafly..."
echo "Connecting to demo-db and building context layer..."

python - << 'PYEOF'
from datafly import Datafly

df = Datafly()
df.connect(
    "postgresql://datafly:datafly@demo-db:5432/demo",
    name="demo_postgres"
)
df.build_context()
print("Context built successfully.")
df.serve(host="0.0.0.0", port=8000)
PYEOF
