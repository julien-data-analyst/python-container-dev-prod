import pytest
import pandas as pd
from src.dagster_informatics_companies.defs.assets.code_naf import get_naf_codes, clean_naf_codes, load_naf_codes
from src.dagster_informatics_companies.defs.jobs import code_naf_job
import dagster as dg
import os
from src.dagster_informatics_companies.defs.resources import PostgresResource

##################################################-
# FIXTURES POUR NOS TESTS
##################################################-
@pytest.fixture
def collecte_naf_codes():
    return get_naf_codes(context = dg.build_asset_context())

@pytest.fixture
def transform_naf_codes():
    return clean_naf_codes(context= dg.build_asset_context())

@pytest.fixture
def context_postgresql():
    return dg.build_op_context(
        resources={
            "postgres": PostgresResource(
            host=os.getenv("HOST_DB"),
            port=os.getenv("PORT_DB"),
            user=os.getenv("USER_DB"),
            password=os.getenv("PASSWORD_DB"),
            database=os.getenv("NAME_DB"),
            )
        }
    )

##################################################-
# TESTS INTEGRATION DES DONNEES
##################################################-
def test_extension_fichier(collecte_naf_codes):
    """
    Voir si on enregistre bien un fichier xls et pas une autre extension
    """
    try:

        # Chemin du fichier après exécution
        chemin = collecte_naf_codes

        # Vérifier extension du fichier Excel
        assert chemin.endswith(".xls")
        
    except Exception as e:
        raise AssertionError("Erreur 'get_naf_codes' pour le test de lecture : ", e)

def test_contenu_fichier(collecte_naf_codes):
    """
    Voir si on a bien du contenu dans ce fichier xls
    """
    try:

        # Chemin du fichier après exécution
        chemin = collecte_naf_codes

        # Lecture du fichier de données
        excel_data = pd.read_excel(chemin)
        
        assert len(excel_data) > 0

    except Exception as e:
        raise AssertionError("Erreur 'get_naf_codes' pour le test de contenu : ", e)
    
def test_entete_fichier(collecte_naf_codes):
    """
    Vérifie que l'en-tête du fichier 'xls' correspond bien à ce qu'on doit s'attendre
    """

    try:

        # Chemin du fichier après exécution
        chemin = collecte_naf_codes

        # Lecture du fichier de données
        en_tete_excel = list(pd.read_excel(chemin).columns)
        
        print("l'en-tête du fichier Excel : ", en_tete_excel)
        assert len(en_tete_excel) >= 3 # Les trois colonnes minimums pour nos transformations
        assert en_tete_excel[0:3] == ["ligne", "Code", " Intitulés de la  NAF rév. 2, version finale "]

    except Exception as e:
        raise AssertionError("Erreur 'get_naf_codes' pour le test d'en-tête : ", e)