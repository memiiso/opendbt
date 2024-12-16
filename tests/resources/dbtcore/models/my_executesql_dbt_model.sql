{{ config(materialized='executesql') }}


create or replace table my_execute_dbt_model
as

select 123 as column1