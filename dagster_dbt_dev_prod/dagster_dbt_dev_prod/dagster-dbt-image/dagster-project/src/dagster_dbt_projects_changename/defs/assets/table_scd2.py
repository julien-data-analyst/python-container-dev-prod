from dagster import asset, AssetExecutionContext
from ..resources import PostgresResource
from ..partitions import categorie_partitions

@asset(
    deps=["companies_cleaned"],
    group_name="scd2",
    partitions_def=categorie_partitions,
    kinds={"Postgres"}
)
def load_companies_scd2(context: AssetExecutionContext, postgres: PostgresResource):
    """
    Gestion de la table SCD2 pour identifier les changements dans les données récoltées par l'API

    :params context: contexte de dagster pour pouvoir enregisrer des logs
    :params postgres: ressource pour se connecter à la base de données PostgreSQL

    :return: table SQL créée dans la base de données PostgreSQL nommée "companies_scd2"
    """
    categorie = context.partition_key

    with postgres.get_connection() as conn:
        cursor = conn.cursor()

        # Créer la table si pas existante
        context.log.info("Création de la table SCD2")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS companies_scd2 (
            siren TEXT,
            nom_raison_sociale TEXT,
            activite_principale TEXT,
            categorie_entreprise TEXT,
            date_creation DATE,
            date_mise_a_jour_insee TIMESTAMP,
            date_mise_a_jour_rne TIMESTAMP,
            nb_etablissements INTEGER,
            nb_etablissements_ouvert INTEGER,
            nature_juridique TEXT,
            tranche_effectif_salarie TEXT,
            adresse TEXT,
            code_postal TEXT,
            commune TEXT,
            departement TEXT,
            region TEXT,
            latitude DECIMAL,
            longitude DECIMAL,
            finances JSONB,
            ca FLOAT,
            resultat_net FLOAT,
            hash_diff TEXT,
            valid_from TIMESTAMP,
            valid_to TIMESTAMP,
            is_current BOOLEAN
        );
        """)

        # Calcul du hash + staging temporaire afin d'identifier s'il y a des différences
        context.log.info("Création de la table temporaire 'tmp_companies_stage'")
        cursor.execute(f"""
        DROP TABLE IF EXISTS tmp_companies_stage;

        CREATE TEMP TABLE tmp_companies_stage AS
        SELECT *,
            md5(
                concat(
                   siren,
                   TRIM(nom_raison_sociale),
                   activite_principale,
                   categorie_entreprise,
                   CAST(date_creation AS text),
                   CAST(date_mise_a_jour_insee AS text),
                   CAST(date_mise_a_jour_rne AS text),
                   COALESCE(CAST(nb_etablissements AS text), ''),
                   COALESCE(CAST(nb_etablissements_ouvert AS text), ''),
                   COALESCE(nature_juridique, ''),
                   COALESCE(tranche_effectif_salarie, ''),
                   TRIM(adresse),
                   code_postal,
                   TRIM(commune),
                   departement,
                   region,
                   COALESCE(CAST(latitude AS text), ''),
                   COALESCE(CAST(longitude AS text), ''),
                   COALESCE(finances::text, '')
                )
            ) AS hash_diff
        FROM companies_cleaned
        WHERE categorie_entreprise='{categorie}';
        """)

        # Détecter les nouvelles entreprises des modifiées
        context.log.info("Détection des nouvelles lignes avec la table 'tmp_changed'")
        cursor.execute(f"""
        CREATE TEMP TABLE tmp_changed AS
        SELECT s.*
        FROM tmp_companies_stage s
        LEFT JOIN companies_scd2 t
            ON s.siren = t.siren
            AND t.is_current = TRUE
        WHERE (t.hash_diff IS NULL
           OR t.hash_diff <> s.hash_diff)
            ;
        """)

        # Fermer anciennes versions
        context.log.info("Mise à jour des versions et fermer ancienne version")
        cursor.execute("""
        UPDATE companies_scd2 t
        SET valid_to = NOW(),
            is_current = FALSE
        FROM tmp_changed c
        WHERE t.siren = c.siren
          AND t.is_current = TRUE;
        """)

        # Insérer nouvelles entreprises et les entreprises modifiées
        context.log.info("Insertion des nouvelles entreprises ou modifiées")
        cursor.execute(f"""
        INSERT INTO companies_scd2 (
            siren,
            nom_raison_sociale,
            activite_principale,
            categorie_entreprise,
            date_creation,
            date_mise_a_jour_insee,
            date_mise_a_jour_rne,
            nb_etablissements,
            nb_etablissements_ouvert,
            nature_juridique,
            tranche_effectif_salarie,
            adresse,
            code_postal,
            commune,
            departement,
            region,
            latitude,
            longitude,
            finances,
            ca,
            resultat_net,
            hash_diff,
            valid_from,
            valid_to,
            is_current
        )
        SELECT
            siren,
            nom_raison_sociale,
            activite_principale,
            categorie_entreprise,
            date_creation,
            date_mise_a_jour_insee,
            date_mise_a_jour_rne,
            nb_etablissements,
            nb_etablissements_ouvert,
            nature_juridique,
            tranche_effectif_salarie,
            adresse,
            code_postal,
            commune,
            departement,
            region,
            latitude,
            longitude,
            finances,
            ca,
            resultat_net,
            hash_diff,
            NOW(),
            NULL,
            TRUE
        FROM tmp_changed;
        """)

        conn.commit()

    context.log.info("SCD2 mis à jour avec succès")