with sources as (
    select * from {{ source('google_sheets','cs_booking') }}
),

renamed as (
    select

        --id
        {{ dbt_utils.surrogate_key(['outlet_code','created_by','phone','booking_hours','booking_date','reservation_date','loaded_date']) }} as unique_id,

        --main dimensions
        outlet_code,
        {{ convert_string_to_null(schema_name='customer_name', adds='') }},
        {{ convert_string_to_null(schema_name='phone', adds='customer_') }},
        {{ convert_string_to_null(schema_name='customer_order', adds='note_') }},
        {{ convert_string_to_null(schema_name='deposit', adds='note_') }},
        {{ convert_string_to_null(schema_name='note', adds='customer_') }},
        {{ convert_string_to_null(schema_name='customer_status', adds='booking_') }},
        {{ convert_string_to_null(schema_name='channel', adds='booking_') }},
        {{ convert_string_to_null(schema_name='zone', adds='booking_') }},

        case when pax1 in('0', '', '#REF!') then null else safe_cast(pax1 as int64) end as number_of_pax1,
        case when pax2 in('0', '', '#REF!') then null else safe_cast(pax2 as int64) end as number_of_pax2,

        {{ convert_string_to_null(schema_name='level_of_customer_satisfaction', adds='') }},
        {{ convert_string_to_null(schema_name='customer_feedback', adds='') }},

        --boolean
        lower(customer_status) = 'yes' as is_success,


        --others
        {{ convert_string_to_null(schema_name='created_by', adds='cs_') }},
        {{ convert_string_to_null(schema_name='cs_call', adds='') }},



        --metadata
        loaded_date as loaded_at,

        --date/time
        safe.parse_datetime('%d/%m/%Y',booking_date) as booking_date,
        coalesce(reservation_date) as reservation_date,

        case
            when length(booking_hours) = 8 then safe.parse_time('%H:%M:%S', booking_hours)
            when booking_hours in (null, '0', '') then null
            else safe.parse_time('%H:%M', booking_hours)
        end as booking_time

    --note used
    -- round,
    -- shift,
    -- weekday,
    -- month,
    -- year,
    -- thoi_gian_goi_,
    from sources
    qualify row_number() over (partition by unique_id order by loaded_date desc) = 1
)

select
    * except (reservation_date,customer_phone),
    case
        when customer_phone is not null then regexp_replace(customer_phone, '^0', '84')
    end as customer_phone,

    case
        when regexp_contains(reservation_date, '/') then safe.parse_datetime('%d/%m/%Y',reservation_date)
    end as reservation_date
from renamed
