{{ config(
    materialized='table'
) }}

/* Nettoyage général des données de l'entreprise */
WITH cleaned_companies AS (
    SELECT
        siren,
        TRIM(nom_raison_sociale) AS nom_raison_sociale,
        activite_principale,
        categorie_entreprise,
        CAST(date_creation AS DATE) AS date_creation,
        CAST(date_mise_a_jour AS TIMESTAMP) AS date_mise_a_jour,
        CAST(date_mise_a_jour_insee AS TIMESTAMP) AS date_mise_a_jour_insee,
        CAST(date_mise_a_jour_rne AS TIMESTAMP) AS date_mise_a_jour_rne,
        CAST(nb_etablissements AS INTEGER) AS nb_etablissements,
        CAST(nb_etablissements_ouverts AS INTEGER) AS nb_etablissements_ouvert,
        nature_juridique,
        CASE tranche_effectif_salarie
            WHEN 'NN' THEN 'Non renseigné' 
            WHEN '00' THEN '0'
            WHEN '01' THEN '1 à 2'
            WHEN '02' THEN '3 à 5'
            WHEN '03' THEN '6 à 9'
            WHEN '11' THEN '10 à 19'
            WHEN '12' THEN '20 à 49'
            WHEN '21' THEN '50 à 99'
            WHEN '22' THEN '100 à 199'
            WHEN '31' THEN '200 à 249'
            WHEN '32' THEN '250 à 499'
            WHEN '41' THEN '500 à 999'
            WHEN '42' THEN '1000 à 1999'
            WHEN '51' THEN '2000 à 4999'
            WHEN '52' THEN '5000 à 9999'
            WHEN '53' THEN '10000+'
            ELSE 'Non renseigné'
        END AS tranche_effectif_salarie,
        TRIM(adresse) AS adresse,
        code_postal,
        TRIM(commune) AS commune,
        departement,
        region,
        CAST(CASE latitude
            WHEN '[NON-DIFFUSIBLE]' THEN NULL
            ELSE latitude
        END AS DECIMAL) AS latitude,
        CAST(CASE longitude
            WHEN '[NON-DIFFUSIBLE]' THEN NULL
            ELSE longitude
        END AS DECIMAL) AS longitude,
        finances,
        ingested_at
    FROM {{ source('codes_nafs_companies', 'companies') }}
    WHERE 
        siren IS NOT NULL
        AND nom_raison_sociale IS NOT NULL
),

/* Récupérer le dernier CA et résultat net de l'entreprise*/
finances_not_null AS (
    SELECT 
        siren,
        finances
    FROM cleaned_companies
    WHERE finances IS NOT NULL
    AND jsonb_typeof(finances) = 'object'
),

latest_financial_year AS (
    SELECT
        siren,
        finances,
        key AS year
    FROM finances_not_null,
    LATERAL (
        SELECT key
        FROM jsonb_each(finances)
        ORDER BY key DESC
        LIMIT 1
    ) sub
),

financials_extracted AS (
    SELECT
        siren,
        year,
        CAST(finances -> year ->> 'ca' AS FLOAT) AS ca,
        CAST(finances -> year ->> 'resultat_net' AS FLOAT) AS resultat_net
    FROM latest_financial_year
)

SELECT
    c.*,
    f.year AS latest_financial_year,
    f.ca,
    f.resultat_net
FROM cleaned_companies c
LEFT JOIN financials_extracted f ON f.siren=c.siren