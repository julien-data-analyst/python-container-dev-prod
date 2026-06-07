from dagster import StaticPartitionsDefinition

# Une partition statique pour les company afin d'optimiser le travail en temps voulu
categorie_partitions = StaticPartitionsDefinition(
    ["PME", "ETI", "GE"]
)