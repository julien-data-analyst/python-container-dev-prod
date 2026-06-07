import pytest
import pandas as pd
from src.dagster_dbt_projects_changename.defs.assets.table_scd2 import load_companies_scd2
import dagster as dg
import os
from src.dagster_dbt_projects_changename.defs.resources import PostgresResource

# Ajouter jobs
def test_scd2_job():
    """
    Vérifie que le job s'exécute correctement
    """
    result = dg.materialize(
        assets=[load_companies_scd2],
        resources={
            "postgres": PostgresResource(
            host=os.getenv("HOST_DB"),
            port=os.getenv("PORT_DB"),
            user=os.getenv("USER_DB"),
            password=os.getenv("PASSWORD_DB"),
            database=os.getenv("NAME_DB"),
            )
        },
        partition_key="GE"
    )

    assert result.success