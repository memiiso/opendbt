with source_data as (
    select 1 as id, 'row1' as row_data
    union all
    select 2 as id, 'row1' as row_data
)

SELECT *
FROM source_data