# ---------------------------
# Stage 1: Builder
# ---------------------------

ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim AS builder 

# Copy uv binary from official image (fast modern dependency manager)
COPY --from=ghcr.io/astral-sh/uv:0.4.20 /uv /bin/uv

# Enable uv to use system Python instead of isolated environment
ENV UV_SYSTEM_PYTHON=1

# Dependencies to install psycopg2 package
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory inside builder container
WORKDIR /app

# Copy only dependency definition first (better Docker layer caching)
COPY pyproject.toml ./

# Copy application source code
COPY dbt-project/ ./dbt-project/

COPY dagster-project/ ./dagster_project/

# Install dependencies using uv (creates environment / installs packages)
RUN uv pip install --system --requirement pyproject.toml


# ---------------------------
# Stage 2: Runtime
# ---------------------------

FROM python:${PYTHON_VERSION}-slim AS runtime 

# Copy uv binary from official image (fast modern dependency manager)
COPY --from=ghcr.io/astral-sh/uv:0.4.20 /uv /bin/uv

# Install git and other dependencies — required by dbt and psycopg2
RUN apt-get update && apt-get install -y \
    libpq5 \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory in final container
WORKDIR /dagster_dbt_project

# Copy installed Python environment from builder stage
COPY --from=builder /usr/local /usr/local

# Copy application source code only (no build tools)
COPY --from=builder /app/dbt-project .
COPY --from=builder /app/dagster_project .
COPY --from=builder /app/pyproject.toml ./dagster_project/

# Ensure Python output is not buffered (useful for logs in Docker)
ENV PYTHONUNBUFFERED=1

# Command to dagster server in dev mode
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]