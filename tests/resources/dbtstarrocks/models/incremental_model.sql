{{
    config(
        materialized='incremental',
        unique_key='event_hash',
        event_time='event_date',
        tags=['bronze'],
    )
}}


with 
 source_data as (
    select md5(concat(cast(event_date as string), event_payload)) as event_hash, 
           event_date, 
           event_payload
    from {{ ref('raw_events_model') }}

    {% if is_incremental() %}
        where event_date > (select max(event_date) from {{ this }})
    {% endif %}
)

select event_hash, event_date, event_payload
from source_data