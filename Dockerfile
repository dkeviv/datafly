FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

# Upgrade build tools first
RUN pip install --upgrade pip "setuptools>=68" wheel --no-cache-dir

# Copy project files
COPY pyproject.toml .
COPY datafly/ ./datafly/

# Install with postgres + openrouter support
RUN pip install ".[postgres,openrouter]" --no-cache-dir

# Context output directory
RUN mkdir -p /app/context

EXPOSE 8000 8080

CMD ["datafly", "serve", "--port", "8000"]
