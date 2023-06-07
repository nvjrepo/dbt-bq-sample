{%- set date_grain=['week','month'] -%}
with orders as (
    select
        date(tran_at) as tran_at,
        bu_3 as company,
        bu,
        outlet_code,
        accessed_email,
        sum(net_sales) as net_sales

    from {{ ref('fct_orders') }}
    {{ dbt_utils.group_by(n=5) }}
),

aggregated as (
    select
        *,
        {% for date_grain in date_grain -%}
            sum(net_sales) over (partition by date_trunc(tran_at, {{ date_grain }}),outlet_code) as {{ date_grain }}ly_net_sales,
        {% endfor %}

        {{ dbt_utils.surrogate_key(['tran_at','outlet_code']) }} as unique_id

    from orders
)

select * from aggregated
