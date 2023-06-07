{{
    config (
        enabled = false
    )
}}

{%- set date_levels = [ 'day',
                       'week',
                       'month'
                      
] -%}

with bis_code as (
    select * from {{ ref('stg_gg_sheet__bis_code') }}
),

bookings as (
    select
        booking_at,
        outlet_code,
        count(*) as number_of_booking,
        sum(pax1) as number_of_booking_pax

    from {{ ref('stg_bookings') }}
    {{ dbt_utils.group_by(n=2) }}
),

final as (
    select
        bookings.*,
        {% for date_level in date_levels -%}
            date_trunc(bookings.booking_at,{{ date_level }}) as booking_{{ date_level }},
        {% endfor %}
        bis_code.bu_1 as bu,
        bis_code.accessed_email
    
    from bookings
    left join bis_code
        on bookings.outlet_code = bis_code.outlet_code
            
)

select * from final