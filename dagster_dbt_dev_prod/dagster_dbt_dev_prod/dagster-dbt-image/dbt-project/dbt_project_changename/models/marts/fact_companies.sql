
{{
config(
    materialized='table'
)
}}
SELECT DISTINCT
        siren, 
        nom_raison_sociale, 
        activite_principale, 
        categorie_entreprise,
        date_creation, 
        CASE
            WHEN date_mise_a_jour_insee > date_mise_a_jour_rne THEN date_mise_a_jour_insee
            ELSE date_mise_a_jour_rne
        END AS date_derniere_mise_a_jour_insee_rne,
        nb_etablissements,
        nb_etablissements_ouvert,
        nb_etablissements - nb_etablissements_ouvert AS nb_etablissements_ferme,
        tranche_effectif_salarie,
        adresse,
        code_postal,
        commune,
        departement AS code_department,
        region AS code_region,
        latitude,
        longitude,
        latest_financial_year,
        ca AS dernier_ca,
        resultat_net AS dernier_resultat_net,
        ingested_at

FROM {{ ref('companies_cleaned') }}
