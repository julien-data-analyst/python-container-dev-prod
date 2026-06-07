
{{
config(
    materialized='table'
)
}}
SELECT *
FROM {{ source('codes_nafs_companies', 'companies_scd2') }}