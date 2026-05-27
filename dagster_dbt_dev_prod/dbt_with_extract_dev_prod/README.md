# dbt with Python Extract Container

## Use Case

This setup is useful for **analytics engineering, data engineering, and ELT pipelines** where **dbt** is the main transformation engine and a separate **Python extraction container** handles data extraction.

It is designed for development and production workflows that combine:

- dbt model execution and transformation
- Python-based data extraction scripts
- containerized orchestration with shared services like PostgreSQL

## Modifying Attributes

If you need to change the dbt project details, update the project name in the folder:

- `dbt-image/dbt-project/dbt_project_changename`

Also update the dbt configuration files accordingly:

- `docker-compose-dev.yml`
- `docker-compose-prod.yml`
- `dbt-image/dbt-project/profiles.yml`
- `dbt-image/dbt-project/dbt_project.yml`

These files must remain consistent so the container can locate and use the correct dbt project and profile settings.

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
docker exec -it dbt-dev bash
```

Then you can run dbt commands inside the container, for example:

```bash
dbt debug    # test database connection
dbt run      # execute dbt models
dbt test     # run dbt tests
```

## Run Python Extraction

To run the Python extraction container in development, use:

```bash
docker exec -it python-dev bash
```

Then start the extraction script:

```bash
cd scripts
python main.py
```
