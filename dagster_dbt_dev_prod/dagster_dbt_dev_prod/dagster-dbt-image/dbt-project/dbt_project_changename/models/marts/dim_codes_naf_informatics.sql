
{{
config(
    materialized='table'
)
}}
SELECT subclasses_codes, subclasses_title, classe_title, classes_codes, section_title
FROM {{ source('codes_nafs_companies', 'code_naf') }}
WHERE classes_codes IN ('62', '63') /* code naf informatique */