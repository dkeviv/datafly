FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

# Install Datafly with Postgres adapter
COPY pyproject.toml .
COPY datafly/ ./datafly/
RUN pip install -e ".[postgres]" --no-cache-dir

# Context output directory
RUN mkdir -p /app/context

EXPOSE 8000 8080

CMD ["datafly", "serve", "--port", "8000"]
