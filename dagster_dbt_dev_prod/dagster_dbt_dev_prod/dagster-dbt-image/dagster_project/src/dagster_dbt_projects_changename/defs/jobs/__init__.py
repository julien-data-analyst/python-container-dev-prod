from dagster import AssetSelection, define_asset_job
from ..partitions import categorie_partitions

# Les assets pour les départements et régions françaises
deps_region_fr = AssetSelection.groups("deps_regions_fr")
dim_region_deps = AssetSelection.assets("dim_region_deps")

# Les assets pour les entreprises
api_company_asset = AssetSelection.assets("extract_load_companies") # Asset d'API (1er job company)
clean_company_asset = AssetSelection.assets("companies_cleaned") # Nettoyage et transformation des données avec DBT (2ème job company)
scd2_company_asset = AssetSelection.assets("load_companies_scd2") # Pour le SCD2 (3ème job company)
mart_company_asset = AssetSelection.assets("dim_companies_scd2", "fact_companies") # Gérer les tables analytiques (4ème job company)

# Les assets pour les codes naf informatiques
code_naf_informatics = AssetSelection.groups("naf_codes")
dim_code_naf_informatics = AssetSelection.assets("dim_codes_naf_informatics")

# Les assets pour le reporting 
reporting_assets = AssetSelection.groups("reporting")

###########################################
# POUR LES ENTREPRISES 
###########################################

# 1er job : collecte et chargement des données des entreprises informatiques françaises
api_company_job = define_asset_job(
    name="api_company_job",
    selection=api_company_asset,
    partitions_def=categorie_partitions
)

# 2ème job : nettoyer et transformer les données
clean_company_job = define_asset_job(
    name="clean_company_job",
    selection=clean_company_asset
)

# 3ème job : gestion de la table SCD2
scd2_company_job = define_asset_job(
    name="scd2_company_job",
    selection=scd2_company_asset,
    partitions_def=categorie_partitions
)

# 4ème job : création des tables analytiques
mart_company_job = define_asset_job(
    name="mart_company_job",
    selection=mart_company_asset
)

##############################################
##############################################

##############################################
# POUR LE REPORTING 
##############################################
reporting_job = define_asset_job(
    name="reporting_job",
    selection=reporting_assets
)
##############################################
##############################################

# Code naf job
code_naf_job = define_asset_job(
    name='code_naf_job',
    selection=(code_naf_informatics | dim_code_naf_informatics)
)

# Région et département job
region_deps_job = define_asset_job(
    name='region_deps_job',
    selection=(deps_region_fr | dim_region_deps)
)