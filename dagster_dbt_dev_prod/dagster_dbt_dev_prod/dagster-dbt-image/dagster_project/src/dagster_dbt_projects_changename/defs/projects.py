# src/dagster_and_dbt/defs/project.py
from pathlib import Path

from dagster_dbt import DbtProject,DbtCliResource

# Indiquer le chemin pour retrouver le fichier manifest.json à partir de l'emplacement du fichier
dbt_project = DbtProject(
  project_dir=Path(__file__).joinpath("../../../../../", "dbt-project", "dbt_project_changename").resolve(),
)

# Actualisation automatique pour indiquer de nouveaux modèles créés ou si les requêtes SQL ont été modifiées
dbt_project.prepare_if_dev()
