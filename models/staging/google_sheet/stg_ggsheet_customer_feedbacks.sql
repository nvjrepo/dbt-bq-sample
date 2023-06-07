with source as (
    select * from {{ source ('google_sheets','customer_feebacks') }}
),

renamed as (
    select
        {{ dbt_utils.surrogate_key(['create_time','customer_name','branch']) }} as unique_id,
        parse_datetime("%d/%m/%Y %H:%M:%S",create_time) as create_time,
        {{ convert_string_to_null('customer_name',adds='') }},
        {{ convert_string_to_null('phone_number',adds='') }},
        {{ convert_string_to_null('branch',adds='') }},
        service_rating_score,
        call_service_rating_score,
        reference_rating_score,

        {{ convert_string_to_null('customer_note',adds='') }},
        {{ convert_string_to_null('cs_note',adds='') }}

    from source
)

select *
from renamed
