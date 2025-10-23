{{ config(materialized='table', table_type='DUPLICATE') }}

with source_data as (
    select 1 as id, 'test-value' as data_value, 'test-value' as column_3
    union all
    select 1 as id, 'test-value' as data_value, 'test-value' as column_3
    union all
    select 2 as id, 'test-value' as data_value, 'test-value' as column_3
)
SELECT *
FROM source_data
-- where id is not null
