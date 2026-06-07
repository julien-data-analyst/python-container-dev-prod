from dagster import sensor, RunRequest, SkipReason, AssetKey, \
    AssetExecutionContext, EventRecordsFilter, DagsterEventType

@sensor(asset_selection=[
    "companies_time_series",
    "companies_by_region",
    "companies_map"
])
def fact_companies_sensor(context: AssetExecutionContext):
    """

    Permet l'actualisation des graphiques de reportings (carte interactive, graphique temporel, graphique région).

    :params context: contexte de dagster pour pouvoir enregisrer des logs, récupérer des métadonnées

    :return: création des trois fichiers reportings trouvable dans le dossier ~/data/reporting/...

    """
    events = context.instance.get_event_records(
       EventRecordsFilter(
        event_type=DagsterEventType.ASSET_MATERIALIZATION, 
        asset_key=AssetKey("fact_companies")
        ),
        limit=1
    )

    if not events:
        return SkipReason("fact_companies n'est pas encore matérialisée")

    # Savoir s'il y a eu un nouvel événement
    last_event_id = events[0].storage_id

    if context.cursor == str(last_event_id):
        return SkipReason("Pas de nouvelle matérialisation")

    context.update_cursor(str(last_event_id))

    return RunRequest(
        run_key=str(last_event_id),
        run_config={}
    )