# Marimo Notebook Container

## Use Case

This container is useful for **data exploration and automated report generation** using Marimo. 
It provides a notebook-based workflow for analyzing datasets and producing reports in a reproducible environment.

## Modifying Attributes

To change the Python version, update both of these files:

- **`.env`**: update `PYTHON_VERSION`
- **`marimo-image/pyproject.toml`**: update `requires-python`

To change Marimo's exposed port on your PC, update the port mapping in **`.env`**.

## Launching the Project

### Development Environment

```bash
docker-compose -f docker-compose-dev.yml up --build
```

### Production Environment

```bash
docker-compose -f docker-compose-prod.yml up --build
```

### Run in Background

```bash
docker-compose -f docker-compose-dev.yml up -d --build
docker-compose -f docker-compose-prod.yml up -d --build
```

### Stop Containers

```bash
docker-compose -f docker-compose-dev.yml down
docker-compose -f docker-compose-prod.yml down
```
