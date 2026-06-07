import requests
from dagster import asset
import os
import pandas as pd
from ..resources import PostgresResource
from .constants import DEPS_SCRIPT, REGIONS_SCRIPT


####################-
# Partie Region load
####################-
@asset(
    group_name="deps_regions_fr",
    kinds={"Postgres"},
)
def load_region_fr(context, postgres: PostgresResource):
    """
    création de la table "departments" avec insertion des données via un script SQL
    dont la source peut se trouver via le lien ci-dessou:


    :params context: contexte de dagster pour pouvoir enregisrer des logs
    :params postgres: ressource pour se connecter à la base de données PostgreSQL

    :return: table SQL créée dans la base de données PostgreSQL nommée "departments"
    """
    
    context.log.info("Script SQL à charger pour les régions françaises")

    # Chargement dans la bdd
    with postgres.get_connection() as conn:
        state_sql = lecture_script_sql(context, REGIONS_SCRIPT, conn)


####################-
# Partie Departement load
####################-
@asset(
    group_name="deps_regions_fr",
    kinds={"Postgres"},
    deps=['load_region_fr']
)
def load_deps_fr(context, postgres: PostgresResource):
    """
    création de la table "departments" avec insertion des données via un script SQL
    dont la source peut se trouver via le lien ci-dessous :
    https://www.data.gouv.fr/datasets/regions-departements-villes-et-villages-de-france-et-doutre-mer


    :params context: contexte de dagster pour pouvoir enregisrer des logs
    :params postgres: ressource pour se connecter à la base de données PostgreSQL

    :return: table SQL créée dans la base de données PostgreSQL nommée "departments"
    """

    context.log.info("Script SQL à charger pour les départements français")

    # Chargement dans la bdd
    with postgres.get_connection() as conn:
        state_sql = lecture_script_sql(context, DEPS_SCRIPT, conn)

def lecture_script_sql(context, chemin_script, connection):
    """
    exécution de requêtes SQL PostgreSQL dans un script donné


    :params context: contexte de dagster pour pouvoir enregisrer des logs
    :params chemin_script: chemin du script à exécuter
    :params connection: objet de connection de PostgreSQL

    :return: retourne une liste de tuple contenant en premier élément une valeur booléenne, en deuxième le nombre de requêtes exécutés et 
             enfin les erreurs rencontrées
    """

    # Lecture du script SQL
    with open(chemin_script, "r", encoding="utf-8") as f:
        sql_script = f.read()

    cursor = connection.cursor()
    statement_sql = sql_script.split(";")
    nb_request_executed = 0
    nb_request_failed = 0
    description_erreurs = []
    for statement in statement_sql:
        if statement.strip():
            try:
                        context.log.info(f"Exécution de la requête : {statement}")
                        nb_request_executed += 1
                        cursor.execute(statement)
                        connection.commit()
            except Exception as e:
                        context.log.error(f"Erreur d'éxécution de la requête : {statement} {e}")
                        nb_request_failed += 1
                        description_erreurs.append([statement, e])
    
    return {
         "state" : True if nb_request_failed==0 else False,
         "request_executed" : nb_request_executed, 
         "request_failed" : nb_request_failed, 
         "description" : description_erreurs
         }


        