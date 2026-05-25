# Python Container - Development & Production

## Use Case

This Python container is designed for **prototyping and executing Python scripts** in both development and production environments. 
It provides a flexible setup with separate configurations for:

- **Development Environment**: For testing, debugging, and development iterations
- **Production Environment**: For stable, optimized script execution

Use this setup to containerize your Python applications with environment-specific configurations while maintaining consistency across deployments.

## Modifying Python Version

To change the Python version, you must update two configuration files:

1. **`.env` file**: Update the `PYTHON_VERSION` variable
   ```
   PYTHON_VERSION=3.11
   ```

2. **`pyproject.toml` file**: Update the Python version requirement in the `[project]` section
   ```toml
   requires-python = ">=3.11"
   ```

Make sure both files are synchronized to avoid version conflicts.

## Launching the Project

### Development Environment

To start the development container:

```bash
docker-compose -f docker-compose-dev.yml up --build
```

### Production Environment

To start the production container:

```bash
docker-compose -f docker-compose-prod.yml up --build
```

### Run in Background

To run either environment in detached mode (background):

```bash
docker-compose -f docker-compose-dev.yml up -d --build
docker-compose -f docker-compose-prod.yml up -d --build
```

### Stop Containers

To stop the running containers:

```bash
docker-compose -f docker-compose-dev.yml down -v
docker-compose -f docker-compose-prod.yml down -v 
```

