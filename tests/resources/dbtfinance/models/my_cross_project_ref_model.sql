
select * from {{ ref('dbtcore', 'my_core_table1') }}

-- fake dependency  {{ source('main', 'my_executepython_model') }}