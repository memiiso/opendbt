select non_exists_column as error_message
from {{ ref('my_first_dbt_model') }}
where id = 1
