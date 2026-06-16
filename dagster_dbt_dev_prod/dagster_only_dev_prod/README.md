# Dagster + dbt Container

## Use Case

This setup is useful for **using Dagster** to manage **multiple ETL/ELT pipelines** in an **analytics engineering** and **data engineering** context.

It is designed for development and production workflows that combine:

- Dagster orchestration for pipeline scheduling and dependency management
- integrated project configuration for multiple pipelines and environments

## Modifying Attributes

If you need to update the project configuration, change these attributes:

- **`.env`**
- `docker-compose-dev.yml`
- `docker-compose-prod.yml`
- `dagster-image/dagster_project/dagster.yaml`
- `dagster-image/dagster_project/workspace.yaml`
- `dagster-image/dagster_project/src/dagster_dbt_projects_changename/` (project folder name)
- `dagster-image/dagster_project/pyproject.toml` (if you change the project name or dependencies)

These files and folders must remain consistent so Dagster can discover the correct project.

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

### Access Dagster UI  

To access the Dagster UI, open http://localhost:3000 in your browser. 
You can view and manage your pipelines, schedules, and resources from there.

### Particularities for the production environment
The prod environment launched very well but you cannot execute some assets for register files.
Why because since it launched a container explicitly for executing thses assets and your assets depends on the local filesytem storage.
If you want to use these files, you need to externalize these ressources on a :
- S3 (the most simple one)
- GCS
- Postgres (JSON storage)
- DuckDB
- Parquet storage
- filesystem manager (IO manager, you need to mount your volumes everywhere)
