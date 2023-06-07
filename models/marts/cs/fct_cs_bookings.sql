with bookings_data as (
    select *
    from {{ ref ('stg_gg_sheet_bookings') }}
),

bis_code as (
    select * from {{ ref('stg_gg_sheet__bis_code') }}
),

booking_logic as (
    select
        bookings_data.*,

        --measurements
        1 as booking_count,

        case
            when is_success then 1
            else 0
        end as succeeded_booking_count,

        --time dimension
        case
            when extract(hour from booking_time) >= 7 and extract(hour from booking_time) <= 14 then 'Morning'
            else 'Evening'
        end as shift,
        case
            when extract(hour from booking_time) >= 6 and extract(hour from booking_time) <= 10 then '1 Breakfast'
            when extract(hour from booking_time) >= 11 and extract(hour from booking_time) <= 14 then '2 Lunch'
            when extract(hour from booking_time) >= 15 and extract(hour from booking_time) <= 18 then '3 Happy Hours'
            when extract(hour from booking_time) >= 19 and extract(hour from booking_time) <= 21 then '4 Dinner'
            else '5 Late Night'
        end as session_round,
        case
            when extract(hour from booking_time) >= 8 and extract(hour from booking_time) < 10 then 'Moderate'
            when booking_time >= '11:30:00' and extract(hour from booking_time) < 13 then 'High'
            when extract(hour from booking_time) >= 13 and extract(hour from booking_time) < 14 then 'Moderate'
            when extract(hour from booking_time) >= 17 and booking_time < '18:30:00' then 'Moderate'
            when booking_time >= '18:30:00' and extract(hour from booking_time) < 21 then 'High'
            else 'Low'
        end as traffic_time

    from bookings_data

)

select
    booking_logic.*,
    bis_code.department_code,
    bis_code.function_code,
    bis_code.department_name,
    bis_code.bu_1 as bu,
    bis_code.bu_2,
    bis_code.bu_3,
    bis_code.zone,
    bis_code.owner,
    bis_code.accessed_email,
    bis_code.district,
    bis_code.function_name,
    bis_code.post_code as post_code,
    bis_code.province,
    bis_code.director,
    bis_code.function_manager,
    bis_code.department_manager,
    bis_code.so_at
from booking_logic
left join bis_code
    on lower(booking_logic.outlet_code) = lower(bis_code.outlet_code)
