import requests
from dagster import asset, AssetExecutionContext
from .constants import API_ENTREPRISES_URL, LISTE_CODE_NAF_VALIDE
import os
import pandas as pd
import time
from ..resources import PostgresResource
from ..partitions import categorie_partitions
from io import StringIO
import json

# Peut-être séparer en deux étapes (collecte et création fichier CSV et ensuite insertion dans la bdd sur ceux nouvelles ou modifiées)
# Voir filtrage temporel 
@asset(
    deps=["load_naf_codes"],
    group_name="companies",
    partitions_def=categorie_partitions,
    kinds={"Postgres", "Python"}
)
def extract_load_companies(context: AssetExecutionContext, postgres: PostgresResource):
    """
    Extraction et chargement direct des données JSON de l'API de recherche d'entreprise.
    Objectif : récupérer les 10 000 plus grosses entreprises par catégorie (PME, ETI, GE)

    :params context: contexte de dagster pour pouvoir enregisrer des logs
    :params postgres: ressource pour se connecter à la base de données PostgreSQL

    :return: table SQL créée dans la base de données PostgreSQL nommée "companies"
    """

    # Récupérer la partition correspondante (ETI, PME, GE)
    categorie = context.partition_key

    # Récupérer la liste des codes NAF informatiques depuis la base
    with postgres.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            Select distinct subclasses_codes 
            From code_naf 
            Where classes_codes in ('62', '63')
        """)
        naf_codes = [row[0] for row in cursor.fetchall()]

    # Préparer la table
    # Modifier pour qu'il prenne en compte que les nouvelles modifications
    with postgres.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            Create table if not exists companies (
                siren Text,
                nom_raison_sociale Text,
                activite_principale Text,
                categorie_entreprise Text,
                date_creation Text,
                date_mise_a_jour Text,
                date_mise_a_jour_insee Text,
                date_mise_a_jour_rne Text,
                nb_etablissements Text,
                nb_etablissements_ouverts Text,
                finances JSONB,
                nature_juridique Text,
                tranche_effectif_salarie Text,
                adresse Text,
                code_postal Text,
                commune Text,
                departement Text,
                region Text,
                latitude Text,
                longitude Text,
                ingested_at Timestamp DEFAULT NOW()
            );
        """)
        cursor.execute("""
        DELETE FROM companies
        WHERE categorie_entreprise = %s
        """, (categorie,))
        conn.commit()

    #########################################
    # 1ERE PARTIE : IDENTIFIER LES CODES VALIDES À RECUPERER
    #########################################
    # Dans notre cas, il existe des codes naf qui ne sont pas valides dans le paramètre "activite_principale"
    # Heureusement, l'API nous montre les codes NAFs valides pour la recherche via une liste que nous l'avons mis dans constants.py
    # Alors l'objectif sera d'abord d'identifier les codes valides et les mettre dans notre liste de paramètre et ensuite exécuter les requêtes pour récupérer les données
    liste_params_valide = []
    for naf_code in naf_codes:
        if naf_code in LISTE_CODE_NAF_VALIDE:
            liste_params_valide.append(naf_code)
    
    if liste_params_valide==[]:
        context.log.error(f"Erreur, aucun code NAF données n'est valide")
        raise ValueError("Erreur, aucun code NAF n'est valide dans la base code_naf")
    
    liste_params_valide_string = ",".join(liste_params_valide)

    ############################################
    # 2EME PARTIE : Collecte des entreprises informatiques
    #############################################
    # Récupérer toutes les entreprises
    all_companies = []
    request_count = 0
        
    # Première requête pour obtenir le nombre total de pages
    params = {
                'activite_principale': liste_params_valide_string,
                'est_entrepreneur_individuel': 'false',
                'sort_by_size' : 'true',
                'categorie_entreprise' : categorie, # Chaque catégorie entreprise, on récupère les plus grosses (10 000)
                "etat_administratif" : "A", # Actif
                'per_page': 25,
                'page': 1
            }
        
        
    context.log.info(f"""Exécution de la première requête avec cette url : {API_ENTREPRISES_URL}?
                        activite_principale={liste_params_valide_string}&
                        est_entrepreneur_individuel={params["est_entrepreneur_individuel"]}&
                        sort_by_size={params["sort_by_size"]}&
                        categorie_entreprise={params["categorie_entreprise"]}&
                        etat_administratif={params["etat_administratif"]}&
                        per_page={params["per_page"]}&
                        page={params["page"]}""")


    # Parourir chaque code et récupérer ses éléments
    if request_count % 7 == 0:
        time.sleep(5)
    response = requests.get(API_ENTREPRISES_URL, params=params)
    request_count += 1
        
    # Dans le cas de la première requête
    if response.status_code != 200:
        context.log.error(f"Erreur API pour {naf_code} et {categorie}: {response.status_code}")
        raise ValueError("Erreur de la première requête de collecte de données")
        
    # Conversion et calcul du nombre de pages à parcourir
    data = response.json()
    total_pages = data.get('total_pages', 1)
    process_page(data, all_companies)

    for page in range(2, total_pages + 1):
                
            if request_count % 7 == 0:
                context.log.info(f"Pause de trois secondes après sept requêtes page {categorie} {page}/{total_pages}.")
                time.sleep(3) # sleep pour eviter spam 7 req
                
            params['page'] = page
            response = requests.get(API_ENTREPRISES_URL, params=params)
            request_count += 1
                
            if response.status_code == 200:
                    data = response.json()
                    process_page(data, all_companies)
            elif response.status_code==429:
                context.log.warning(f"Pause de trois secondes, trop de requêtes pour l'API {categorie} {page}/{total_pages}")
                time.sleep(3)
                page -= 1 # Revenir en arrière pour l'exécuter
            else:
                    context.log.error(f"Erreur page {page} pour {liste_params_valide_string} {categorie} : {response.status_code}")
    
    ############################################
    # 3EME PARTIE : CHARGEMENT DANS LA BDD
    ############################################

    # Charger en base
    if all_companies:
        df = pd.DataFrame(all_companies)
        # df.to_csv(CHEMIN_TEMPORAIRE_ENTREPRISE,index=False)

        # Remplacer les [NON DIFFUSIBLES]
        with postgres.get_connection() as conn:
            cursor = conn.cursor()
            
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            
            cursor.copy_expert(
                sql="""
                    Copy companies (
                        siren,
                        nom_raison_sociale,
                        activite_principale,
                        categorie_entreprise,
                        date_creation,
                        date_mise_a_jour,
                        date_mise_a_jour_insee,
                        date_mise_a_jour_rne,
                        nb_etablissements,
                        nb_etablissements_ouverts,
                        finances,
                        nature_juridique,
                        tranche_effectif_salarie,
                        adresse,
                        code_postal,
                        commune,
                        departement,
                        region,
                        latitude,
                        longitude
                    )
                    from stdin with csv
                """,
                file=buffer
            )
            conn.commit()
    else:
        context.log.warning("Aucune entreprise trouvé")


def process_page(data, companies_list):
    """
    Permet d'extraire les données d'un JSON donné pour l'entreprise.

    :params data: réponse de la requête API pour la collecte de données
    :params companies_list: liste des entreprises qu'on va utiliser pour ajouter les données de chaque entreprise

    :return: Ajout des données dans "companies_list"
    """
    for result in data.get('results', []):

        # Gérer le cas si les informations se trouve dans unite_legale (possible si le filtrage nous envoie la donnée)
        presence_unite_legale = True
        unite_legale = result.get('unite_legale', {})

        if unite_legale == {}:
            unite_legale = result
            presence_unite_legale = False


        siege = result.get('siege', {})
        finances = result.get('finances', {})
        
        company = {
            'siren': unite_legale.get('siren'),
            'nom_raison_sociale': unite_legale.get('denomination') or unite_legale.get('nom') or unite_legale.get('prenom') if presence_unite_legale else unite_legale.get('nom_raison_sociale'),
            'activite_principale': unite_legale.get('activite_principale'),
            'categorie_entreprise': unite_legale.get('categorie_entreprise'),
            'date_creation': unite_legale.get('date_creation'),
            'date_mise_a_jour': unite_legale.get('date_mise_a_jour'),
            'date_mise_a_jour_insee': unite_legale.get('date_mise_a_jour_insee'),
            'date_mise_a_jour_rne' : unite_legale.get('date_mise_a_jour_rne'),
            'nb_etablissements' : unite_legale.get('nombre_etablissements'),
            'nb_etablissements_ouverts' : unite_legale.get('nombre_etablissements_ouverts'),
            'finances' : json.dumps(finances),
            'nature_juridique': unite_legale.get('nature_juridique'),
            'tranche_effectif_salarie': unite_legale.get('tranche_effectif_salarie'),
            'adresse': siege.get('adresse'),
            'code_postal': siege.get('code_postal'),
            'commune': siege.get('libelle_commune'),
            'departement' : siege.get('departement'),
            'region' : siege.get('region'),
            'latitude': siege.get("latitude"),
            'longitude': siege.get("longitude")
        }
        companies_list.append(company)