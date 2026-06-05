from dagster import asset, AssetExecutionContext
from ..resources import PostgresResource
import pandas as pd
from .constants import TEMPORAL_GRAPH, BAR_REGION_GRAPH, CARTE_GRAPH
from plotnine import ggplot, aes, geom_line, labs, theme_bw, theme, geom_text, position_dodge, element_blank, geom_bar, scale_x_discrete, element_text
import plotly.express as px

@asset(
       deps=["fact_companies"],
       group_name="reporting",
       kinds={"Python"})
def companies_time_series(context: AssetExecutionContext, postgres: PostgresResource):
    """
    Création d'une courbe temporelle permettant d'observer par année le nombre de création d'entreprise informatique.

    :params context: contexte de dagster pour pouvoir enregisrer des logs
    :params postgres: ressource pour se connecter à la base de données PostgreSQL

    :return: graphique temporelle sur le nombre de création d'entreprise par année
    """

    # Requête pour récuperer les dates de création de chaque companie
    query = """
        SELECT 
            date_part('year', date_creation) AS year,
            COUNT(siren) AS nb_companies
        FROM fact_companies
        WHERE date_part('year', date_creation) >= 2000
        GROUP BY date_part('year', date_creation)
        ;
    """
    
    # Connection à la base de données
    with postgres.get_connection() as conn:
        df = pd.read_sql(query, conn)

    # Aggrégation par year et calcul du nombre d'entreprises par année
    agg_df = df.sort_values("year")

    # Création du graphique
    plot = (
        ggplot(agg_df, aes(x="year", y="nb_companies"))
        + geom_line()
        + labs(
            title="Nombre d'entreprises informatique créées par année",
            x="Année",
            y="Nombre d'entreprises"
        )
        + geom_text(
                aes(label="nb_companies"),
                position=position_dodge(width=0.9),
                size=8,
                va="bottom"
            )
        + theme_bw()
        + theme(figure_size=(10, 8), # Taille de la figure
                        panel_grid= element_blank()) # Enlever les carreaux gris
    )

    output_path = TEMPORAL_GRAPH
    plot.save(output_path, width=12, height=10, dpi=300)

    context.log.info(f"Temporal graph saved at {output_path}")

@asset(
        deps=["fact_companies", "dim_region_deps"],
        group_name="reporting",
        kinds={"Python"})
def companies_by_region(context: AssetExecutionContext, postgres: PostgresResource):
    """
    Création d'un graphique en barre sur le nombre d'entreprises par région (top 10)

    :params context: contexte de dagster pour pouvoir enregisrer des logs
    :params postgres: ressource pour se connecter à la base de données PostgreSQL

    :return: graphique en barre sur le nombre d'entreprises par région (top 10)
    """
     
    query = """
        SELECT r.region_name, COUNT(f.siren) AS nb_companies
        FROM fact_companies f
        LEFT JOIN dim_region_deps r
        ON r.code_region = f.code_region
        WHERE r.code_region IS NOT NULL
        GROUP BY r.region_name
        ;
    """
    
    # Connection à la base de données
    with postgres.get_connection() as conn:
        df = pd.read_sql(query, conn)
        
    liste_discrete = list(df.sort_values(by="nb_companies", ascending=False)["region_name"])

    plot = (
        ggplot(df, aes(x="region_name", y="nb_companies"))
        + geom_bar(
            color="black",
            fill="blue",
            stat="identity")
        + labs(
            title="Nombre d'entreprises informatiques par région",
            x="Région",
            y="Nombre d'entreprises"
        )
        + geom_text(
                aes(label="nb_companies"),
                position=position_dodge(width=0.9),
                size=8,
                va="bottom"
            )
        + theme_bw()
        + scale_x_discrete(limits=liste_discrete) # Ajout d'un ordre (décroissant)
        + theme(figure_size=(10, 8), # Taille de la figure
                panel_grid= element_blank(), # Enlever les carreaux gris
                axis_text_x=element_text(rotation=45, ha="right")) # Texte de l'axe X ambié
    )

    output_path = BAR_REGION_GRAPH
    plot.save(output_path, width=10, height=5, dpi=300)

    context.log.info(f"Bar chart saved at {output_path}")

@asset(
        deps=["fact_companies", "dim_codes_naf_informatics"],
        group_name="reporting",
        kinds={"Python"}
        )
def companies_map(context: AssetExecutionContext, postgres: PostgresResource):
    """
    Création d'une carte HTML sur les entreprises informatiques implémentées dans le pays

    :params context: contexte de dagster pour pouvoir enregisrer des logs
    :params postgres: ressource pour se connecter à la base de données PostgreSQL

    :return: carte HTML sur les entreprises informatiques implémentées dans le pays
    """

    query = """
        SELECT 
            f.latitude,
            f.longitude,
            f.categorie_entreprise,
            f.nom_raison_sociale,
            f.date_creation,
            f.tranche_effectif_salarie,
            f.adresse,
            cni.subclasses_title AS activite_principale,
            cni.classe_title AS classe_activite
        FROM fact_companies f
        LEFT JOIN dim_codes_naf_informatics cni
        ON cni.subclasses_codes = f.activite_principale
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ;
    """
    
    # Connection à la base de données
    with postgres.get_connection() as conn:
        df = pd.read_sql(query, conn)

    # Couleur selon la catégorie de l'entreprise
    color_map = {
        "GE": "red",
        "PME": "green",
        "ETI": "orange"
    }

    # Création de la map
    fig = px.scatter_mapbox(
        df,
        lat="latitude",
        lon="longitude",
        color="categorie_entreprise",
        color_discrete_map=color_map,
        hover_data={
            "nom_raison_sociale": True,
            "date_creation": True,
            "tranche_effectif_salarie": True,
            "adresse": True,
            "activite_principale": True,
            "classe_activite" : True
        },
        zoom=5,
        height=700
    )

    # Définir le type de carte
    fig.update_layout(mapbox_style="carto-positron")

    output_path = CARTE_GRAPH
    fig.write_html(output_path)

    context.log.info(f"Map saved at {output_path}")