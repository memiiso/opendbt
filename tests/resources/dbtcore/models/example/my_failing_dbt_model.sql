select non_exists_column as my_failing_column
from {{ ref('my_first_dbt_model') }}
where id = 1
