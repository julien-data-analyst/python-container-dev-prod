# ---------------------------
# Build and Run
# ---------------------------

ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Copy uv binary from official image (fast modern dependency manager)
COPY --from=ghcr.io/astral-sh/uv:0.4.20 /uv /bin/uv

# Set working directory inside builder container
WORKDIR /app

# Copy only dependency definition first (better Docker layer caching)
COPY pyproject.toml ./

# Copy application source code
COPY scripts/ ./scripts/

# Install dependencies using uv (creates environment / installs packages)
RUN uv sync

# Ensure Python output is not buffered (useful for logs in Docker)
ENV PYTHONUNBUFFERED=1

# Command to start the notebook server when the container runs
CMD ["uv", "run", "marimo", "edit", "--no-token", "--host", "0.0.0.0", "-p", "8080"]