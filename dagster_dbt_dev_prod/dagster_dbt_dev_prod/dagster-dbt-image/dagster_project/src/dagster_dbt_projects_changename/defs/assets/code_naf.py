import requests
from dagster import asset
from .constants import URL_GET_EXCEL_NAF_CODES, CODE_NAF_RAW_PATH, CODE_NAF_STAGING_PATH
import os
import pandas as pd
import numpy as np
from ..resources import PostgresResource
from io import StringIO

######################################################################-
# Pipeline ETL : capture des codes nafs et de leurs intitulés
######################################################################-

####################-
# Partie Extract
####################-
@asset(
    group_name="naf_codes",
    kinds={"python"},
)
def get_naf_codes(context):
    """
    Extraction des codes nafs du fichier xls de l'INSEE

    :params context: contexte de dagster pour pouvoir enregisrer des logs

    :return: fichier xls stockant les codes nafs et leurs intitulés
    """
    
    try:
        context.log.info("Début du téléchargement des codes NAF")

        nafs_codes = requests.get(
            URL_GET_EXCEL_NAF_CODES
        )

        if nafs_codes.status_code != 200 :
            raise ValueError("Erreur lors de l'envoie de la requête pour récupérer Excel : "+str(e))
        contenu = nafs_codes.content
        with open(CODE_NAF_RAW_PATH, "wb") as output_file:
            output_file.write(contenu)

        return CODE_NAF_RAW_PATH
    
    except Exception as e:
        context.log.error("Erreur au niveau de la requête pour les codes naf : "+str(e))
        raise ValueError("Erreur au niveau de la requête pour l'Excel du code NAF : "+str(e))
    

####################-
# Partie Transform
####################-


@asset(
        deps=["get_naf_codes"],
        group_name="naf_codes",
        kinds={"python"},
)
def clean_naf_codes(context):
    """
    nettoyage des données des codes nafs

    :params context: contexte de dagster pour pouvoir enregisrer des logs

    :return: fichier csv nettoyé des codes nafs avec leurs intitulés, les classes et sections
    """
    
    context.log.info("Début du nettoyage des codes NAFs")

    # Read the excel file with the raw path
    excel_data = pd.read_excel(CODE_NAF_RAW_PATH)

    # Filter the lines with null values on "Code" colup
    excel_data_non_nulles = excel_data[~excel_data["Code"].isna()]
    excel_data_non_nulles.drop(excel_data_non_nulles.columns[3:5], axis=1, inplace=True)

    # Séparer les sections et classes des sous-classes
    excel_data_section_classes = excel_data_non_nulles[~excel_data_non_nulles["Code"].str.contains("\.")]

    # a loop for in each line because we need the previous values (SECTION) to add new values
    liste_dataframes_values = [] # ["Code_class", "title_class", "title_section"]
    context.log.info("Préparation des classes et sections")
    for lines in excel_data_section_classes.values:
        
        # If section line, take the values of the actual section
        if "section" in lines[1].lower():
            context.log.info("Section : "+lines[2])
            section_act = lines[2].strip()
        else:
            liste_dataframes_values.append([lines[1].strip(), lines[2].strip(), section_act])


    # Obtain the subclasses
    context.log.info("Préparation des sous-classes et joindre les classes et sections avec")
    subclass_naf = excel_data_non_nulles[excel_data_non_nulles["Code"].str.contains("\.")]
    subclass_naf.columns = ["excel_line", "subclasses_codes", "subclasses_title"]

    # Clean the strings
    subclass_naf["subclasses_codes"] = subclass_naf["subclasses_codes"].str.strip()
    subclass_naf["subclasses_title"] = subclass_naf["subclasses_title"].str.strip().str.lower()

    # Utility function for the apply method
    def find_classes_and_section(row):
        string_researched = row["subclasses_codes"].split(".")[0]
        found = False
        
        for elt_rech in liste_dataframes_values:
            # print(elt_rech[0])
            # print(string_researched)
            if elt_rech[0] == string_researched:
                found = True
                row["classes_codes"] = elt_rech[0]
                row["classe_title"] =elt_rech[1]
                row["section_title"] = elt_rech[2]

        if found!=True:
            row["classes_codes"] = np.nan
            row["classe_title"] =np.nan
            row["section_title"] = np.nan

        return row

    # Apply the function to get information about classes and sections
    dataframe_reunited = subclass_naf.apply(find_classes_and_section, axis=1)

    # Export to CSV which we are going to load into a SQL table afterwards
    dataframe_reunited.to_csv(CODE_NAF_STAGING_PATH,
                              sep=";",
                              header=True,
                              index=False)
    
    return CODE_NAF_STAGING_PATH


####################-
# Partie Load
####################-
@asset(
    deps=["clean_naf_codes"],
    group_name="naf_codes",
    kinds={"Postgres"},
)
def load_naf_codes(context, postgres: PostgresResource):
    """
    chargement des données csv dans une table SQL PostgreSQL

    :params context: contexte de dagster pour pouvoir enregisrer des logs
    :params postgres: ressource pour se connecter à la base de données PostgreSQL

    :return: table SQL créée dans la base de données PostgreSQL nommée "code_naf"
    """
    # Lecture du fichier CSV
    dataframe_load = pd.read_csv(CODE_NAF_STAGING_PATH,
                                 sep=";",
                                 header=0)
    
    # Chargement dans la bdd
    with postgres.get_connection() as conn:
        cursor = conn.cursor()

        context.log.info("Drop / create table")

        cursor.execute("""
            DROP TABLE IF EXISTS code_naf;
            CREATE TABLE code_naf (
                excel_line INTEGER,
                subclasses_codes TEXT,
                subclasses_title TEXT,
                classes_codes TEXT,
                classe_title TEXT,
                section_title TEXT
            );
        """)

        # Conversion DataFrame → buffer CSV en mémoire
        buffer = StringIO()
        dataframe_load.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        context.log.info("COPY en cours du fichier CSV")

        cursor.copy_expert(
            sql="""
                COPY code_naf (
                    excel_line,
                    subclasses_codes,
                    subclasses_title,
                    classes_codes,
                    classe_title,
                    section_title
                )
                FROM STDIN WITH CSV
            """,
            file=buffer
        ) # Exécution de la requête SQL pour copier-coller un fichier CSV dans la base de données

        conn.commit()

        context.log.info(f"{len(dataframe_load)} lignes chargées avec succès")