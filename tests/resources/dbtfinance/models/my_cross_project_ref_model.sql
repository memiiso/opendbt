
select
 id,
 row_data,
 count(*) as num_rows
from {{ ref('dbtcore', 'my_core_table1') }}
-- fake second dependency  {{ source('core', 'my_executepython_model') }}
group by 1,2