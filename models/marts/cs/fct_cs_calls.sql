with call_data as (
    select *
    from {{ ref ('stg_worldfone_cdrs') }}
),

key_metrics as (
    select
        *,
        disposition_status != 'answered' as is_missed_call,
        case
            when direction_type = 'inbound' then 1
            else 0
        end as inbound_call,
        case
            when direction_type = 'outbound' then 1
            else 0
        end as outbound_call
    from call_data
)

select *
from key_metrics
