
{{
config(
    materialized='table'
)
}}
SELECT reg.code AS code_region, reg.name AS region_name, 
        dep.code AS code_department, dep.name AS department_name
FROM {{ source('codes_nafs_companies', 'regions') }} AS reg
LEFT JOIN {{ source('codes_nafs_companies', 'departments') }} AS dep
ON dep.region_code = reg.code