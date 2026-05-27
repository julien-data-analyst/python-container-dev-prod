# dbt-only Container

## Use Case

This container is useful for **analytics engineering, data engineering, and ELT pipelines** with **dbt** as the main core. It is intended for building and running dbt models in a containerized environment for both development and production workflows.

## Modifying Attributes

If you need to change the dbt project details, update the project name in the folder:

- `dbt-image/dbt-project/dbt_project_changename`

Also update the dbt configuration files accordingly:

- `docker-compose-dev.yml`
- `docker-compose-prod.yml`
- `dbt-image/profiles.yml`
- `dbt-image/dbt-project/dbt_project.yml`

For the configuration of the database connection, profiles.yml is set up to use environment variables. You can modify the database connection details in the `.env` file:

```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=datawarehouse
POSTGRES_PORT=5434
HOST_DB=postgres_db_dbt
TYPE_DB=postgres
```

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

To launch the dbt models, enter the dev container with:

```bash
docker exec -it dbt-dev bash
```

Then you can run dbt commands inside the container, for example:

```bash
dbt debug    # test database connection
dbt run      # execute dbt models
dbt test     # execute dbt tests
```