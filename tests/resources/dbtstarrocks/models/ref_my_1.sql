{{
    config(
        materialized='table'
    )
}}

select * from {{ ref('my_first_dbt_model') }}