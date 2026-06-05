from dagster import ScheduleDefinition
from ..jobs import api_company_job, region_deps_job, code_naf_job, clean_company_job, scd2_company_job, mart_company_job

# Exécution annuelle pour les codes (début d'année)
naf_codes_annual = ScheduleDefinition(
    job=code_naf_job,
    cron_schedule="0 0 5 1 *", # Tous les cinq janvier de l'année
    execution_timezone="Europe/Paris",
)

# Exécution annuelle pour les régions départements (début d'année)
region_deps_annual = ScheduleDefinition(
    job=region_deps_job,
    cron_schedule="0 0 5 1 *", # Tous les cinq janvier de l'année
    execution_timezone="Europe/Paris",
)

# Exécution journalière pour l'API
api_companies_dayli = ScheduleDefinition(
    job=api_company_job,
    cron_schedule="0 0 * * *", # Tous les jours à minuit
    execution_timezone="Europe/Paris",
)

# Exécution journalière une heure plus tard pour la transformation
clean_companies_dayli = ScheduleDefinition(
    job=clean_company_job,
    cron_schedule="0 0 1 * *", # Tous les jours à une heure
    execution_timezone="Europe/Paris",
)

# Exécution journalière deux heures plus tards pour le SCD2
scd2_companies_dayli = ScheduleDefinition(
    job=scd2_company_job,
    cron_schedule="0 0 2 * *", # Tous les jours à deux heures
    execution_timezone="Europe/Paris",
)

# Exécution journalière des tables analytiques trois heures plus tards
mart_companies_dayli = ScheduleDefinition(
    job=mart_company_job,
    cron_schedule="0 0 3 * *", # Tous les jours à trois heures
    execution_timezone="Europe/Paris",
)