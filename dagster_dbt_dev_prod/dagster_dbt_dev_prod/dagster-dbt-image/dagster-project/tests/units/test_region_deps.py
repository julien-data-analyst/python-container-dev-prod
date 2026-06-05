import pytest
import pandas as pd
from src.dagster_dbt_projects_changename.defs.assets.region_deps import load_deps_fr, load_region_fr
import dagster as dg
import os
from src.dagster_dbt_projects_changename.defs.resources import PostgresResource

##################################################-
# FIXTURES POUR NOS TESTS
##################################################-
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
    
############################################################-
# TESTS UNITAIRES POUR LE CHARGEMENT DES DONNEES 
############################################################-

def test_entete_regions(context_postgresql):
    """
    Vérifie que la table PostgreSQL a la bonne structure
    """

    try:
        # Exécuter l'asset
        load_region_fr(context=context_postgresql)

        # Accéder à la ressource via le contexte
        with context_postgresql.resources.postgres.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = 'regions'
                ORDER BY ordinal_position;
            """)

            noms_types_colonnes = [row for row in cursor]

        expected = [
            ("id",),
            ("code",),
            ("name",),
            ("slug",)
        ]

        #print("Les noms et types des colonnes : ", noms_types_colonnes)

        assert noms_types_colonnes == expected
    
    except Exception as e:
        raise AssertionError("Erreur 'load_region_fr' pour le test de l'en-tête : ", e)

def test_contenu_regions(context_postgresql):
    """
    Vérifie que la table PostgreSQL a du contenu dans la base de données
    """

    try:
        # Exécuter l'asset
        load_region_fr(context=context_postgresql)

        # Accéder à la ressource via le contexte
        with context_postgresql.resources.postgres.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT COUNT(id)
                FROM regions
            """)

            count = [row for row in cursor]
            #print("Nombre de lignes : ", count)
            assert count[0][0] > 0

    except Exception as e:
        raise AssertionError("Erreur 'load_region_fr' pour le test du contenu dans la bdd : ", e)

def test_entete_deps(context_postgresql):
    """
    Vérifie que la table PostgreSQL a la bonne structure
    """

    try:
        # Exécuter l'asset
        load_deps_fr(context=context_postgresql)

        # Accéder à la ressource via le contexte
        with context_postgresql.resources.postgres.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = 'departments'
                ORDER BY ordinal_position;
            """)

            noms_types_colonnes = [row for row in cursor]

        expected = [
            ("id",),
            ("region_code",),
            ("code",),
            ("name",),
            ("slug",)
        ]

        #print("Les noms et types des colonnes : ", noms_types_colonnes)

        assert noms_types_colonnes == expected
    
    except Exception as e:
        raise AssertionError("Erreur 'load_deps_fr' pour le test de l'en-tête : ", e)

def test_contenu_regions(context_postgresql):
    """
    Vérifie que la table PostgreSQL a du contenu dans la base de données
    """

    try:
        # Exécuter l'asset
        load_region_fr(context=context_postgresql)

        # Accéder à la ressource via le contexte
        with context_postgresql.resources.postgres.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT COUNT(id)
                FROM departments
            """)

            count = [row for row in cursor]
            #print("Nombre de lignes : ", count)
            assert count[0][0] > 0

    except Exception as e:
        raise AssertionError("Erreur 'load_deps_fr' pour le test du contenu dans la bdd : ", e)

# Ajouter jobs
def test_region_deps_job():
    """
    Vérifie que le job s'exécute correctement
    """
    result = dg.materialize(
        assets=[load_deps_fr, load_region_fr],
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

    assert result.success