with call_data as (
    select *
    from {{ ref ('stg_worldfone_cdrs') }}
),

key_metrics as (
    select
        *,
        case
            when disposition_status != 'answered' then 1
            else 0
        end as missed_call,
        case
            when direction_type = 'inbound' then 1
            else 0
        end as inbound_call,
        case
            when direction_type != 'outbound' then 1
            else 0
        end as outbound_call
    from call_data
)

select *
from key_metrics
