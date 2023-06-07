{%- set metrics=[
            'tc',
            'customer',
            'promotion',
            'discount',
            'male',
            'female'

] -%}

with order_items as (
    select * from {{ ref('int_sales') }}
),

/*
booking as (
    select * from ref('stg_bookings')
),

we_expenses as (
    select * from ref('stg_we_expenses')
),

booking_metric as (
    select
        outlet_code,
        date_trunc(booking_at,day) as tran_at,
        'booking_pax' as metric_names,
        sum(pax1) as metric_value
    from booking
    {{ dbt_utils.group_by(n=3) }}

    union all

    select
        outlet_code,
        date_trunc(booking_at,day) as tran_at,
        'booking' as metric_names,
        count(*) as metric_value
    from booking
    {{ dbt_utils.group_by(n=3) }}
),

we_metric as (
    select
        outlet_code,
        date_trunc(event_at,day) as tran_at,
        'water_fee' as metric_names,
        round(sum(water_cost)/1000,0) as metric_value
    from we_expenses
    {{ dbt_utils.group_by(n=3) }}

    union all

    select
        outlet_code,
        date_trunc(event_at,day) as tran_at,
        'electric_fee' as metric_names,
        round(sum(electric_cost)/1000,0) as metric_value
    from we_expenses
    {{ dbt_utils.group_by(n=3) }}
),
*/

{% for metric in metrics -%}

    {{ metric }}_metric as (
        select
            outlet_code,
            date_trunc(tran_at,day) as tran_at,
            '{{ metric }}' as metric_names,

            {% if metric == 'customer' -%}
            sum(total_customers) as metric_value
{% else %}
                sum({{ metric }}) as metric_value
            {% endif %}
        from order_items
        {{ dbt_utils.group_by(n=3) }}
    ),

{% endfor %}

duration_metric as (
    select
        outlet_code,
        date_trunc(tran_at,day) as tran_at,
        'duration' as metric_names,
        avg(duration) as metric_value

    from order_items
    {{ dbt_utils.group_by(n=3) }}
),

unioned as (
    select * from tc_metric
    union all
    select * from customer_metric
    union all
    select * from promotion_metric
    union all
    select * from discount_metric
    union all
    select * from male_metric
    union all
    select * from female_metric
    union all
    select * from duration_metric
)

select * from unioned
