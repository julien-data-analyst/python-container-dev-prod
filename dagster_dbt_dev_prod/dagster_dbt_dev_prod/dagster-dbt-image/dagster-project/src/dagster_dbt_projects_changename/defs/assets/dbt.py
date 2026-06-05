# Load librairies
from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator
from ..projects import dbt_project
from ..partitions import categorie_partitions

CODE_NAF_SELECTOR = "code_naf"

class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    """
    Cette classe nous permet de gérer les dépendances de nos dbt notamment si on doit les lier avec des assets Dagster
    """
    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]

        if resource_type == 'source' and name=="code_naf":
            return AssetKey("load_naf_codes")
        elif resource_type == 'source' and name=="companies":
            return AssetKey("extract_load_companies")
        elif resource_type == 'source' and name=="departments":
            return AssetKey("load_deps_fr")
        elif resource_type == 'source' and name=='companies_scd2':
            return AssetKey("load_companies_scd2")
        else:
            return super().get_asset_key(dbt_resource_props)
    
    def get_group_name(self, dbt_resource_props):
        return dbt_resource_props["fqn"][1]

# Chaque asset que vous trouverez ci-dessous permets de mieux gérer selon le tag les différents éléments dbt à exécuter.
# Cela permet de séparer les responsabilités entre les différents modèles dbt

# Pour les codes nafs
@dbt_assets(
    manifest=dbt_project.manifest_path, # Indicates where is placed the manifest
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    select="tag:codes_naf"
)
def dbt_code_naf(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# Pour les régions et départements
@dbt_assets(
    manifest=dbt_project.manifest_path, # Indicates where is placed the manifest
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    select="tag:region_deps"
)
def dbt_region_deps(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# Pour les transformations en entreprises
@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    select="tag:staging_company"
)
def dbt_companies(context: AssetExecutionContext, dbt: DbtCliResource):

    yield from dbt.cli(
        ["build"],
        context=context
    ).stream()

# Pour les tables analytics companies/scd2
@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    select="tag:mart_company"
)
def dbt_marts(context: AssetExecutionContext, dbt: DbtCliResource):

    yield from dbt.cli(
        ["build"],
        context=context
    ).stream()