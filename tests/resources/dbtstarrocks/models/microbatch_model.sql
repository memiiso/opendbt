{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        unique_key='event_hash',
        event_time='event_date',
        begin='2025-10-08',
        batch_size='day',
    )
}}

with source_data as (
    select md5(concat(cast(event_date as string), event_payload)) as event_hash,
        event_date, event_payload
    from {{ ref('incremental_model') }}
)

select
    event_hash,
    event_date,
    event_payload as payload
from source_data