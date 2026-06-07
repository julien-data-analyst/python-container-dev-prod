# Dagster + dbt Container

## Use Case

This setup is useful for **using Dagster with dbt** to manage **multiple ELT pipelines** in an **analytics engineering** and **data engineering** context.

It is designed for development and production workflows that combine:

- Dagster orchestration for pipeline scheduling and dependency management
- dbt for data transformation and analytics engineering
- integrated project configuration for multiple pipelines and environments

## Modifying Attributes

If you need to update the project configuration, change these attributes:

- **`.env`**
- `docker-compose-dev.yml`
- `docker-compose-prod.yml`
- `dagster-dbt-image/dagster_project/dagster.yaml`
- `dagster-dbt-image/dagster_project/workspace.yaml`
- `dagster-dbt-image/dagster_project/src/dagster_dbt_projects_changename/` (project folder name)
- `dagster-dbt-image/dbt-project/dbt_project_changename/profiles.yml`
- `dagster-dbt-image/dbt-project/dbt_project_changename/dbt_project.yml`
- `dagster-dbt-image/dagster_project/pyproject.toml` (if you change the project name or dependencies)

These files and folders must remain consistent so Dagster can discover the correct project, and dbt can use the correct profile and project settings.

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

## Launch dbt Models

To launch the dbt models, enter the dbt dev container:

```bash
docker exec -it dagster-dbt-dev bash
```

Then run commands inside the container, for example:

```bash
dbt debug    # test database connection
dbt run      # execute dbt models
dbt test     # run dbt tests
```

### Access Dagster UI  

To access the Dagster UI, open http://localhost:3000 in your browser. 
You can view and manage your pipelines, schedules, and resources from there.
