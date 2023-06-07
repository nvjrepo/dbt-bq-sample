with sources as (
    select * from {{ source('worldfone','cdrs') }}
),

renamed as (
    select
        --id
        {{ dbt_utils.surrogate_key(['accountcode','uniqueid','calldate']) }} as unique_id,

        --dim
        src as calling_number, --so nguoi goi
        accountcode, --ten nguoi goi
        dst as received_number, --so bi goi
        lower(disposition) as disposition_status,
        did_number,
        direction as direction_type,
        carrier as carrier_name,

        --measure
        duration as duration_time,
        billsec as bill_sec,
        holdtime as hold_time,
        waitingtime as waiting_time,

        --datetime
        datetime(calldate) as call_at,

        --others
        queue,
        loaded_date as loaded_at
    from sources
)

select *


from renamed
