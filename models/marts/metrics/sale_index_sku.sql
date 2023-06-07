{%- set date_grain=['week','month'] -%}
with orders as (
    select
        date(tran_at) as tran_at,
        sku_code,
        item_name,
        category,
        item_group,
        bu_3 as company,
        bu,
        outlet_code,
        accessed_email,
        sum(quantity) as quantity,
        sum(net_sales) as net_sales

    from {{ ref('fct_order_line_items') }}
    {{ dbt_utils.group_by(n=9) }}
),

aggregated as (
    select
        *,
        sum(quantity) over (partition by date_trunc(tran_at, day),outlet_code) as daily_quantity,
        {% for date_grain in date_grain -%}
            sum(quantity) over (partition by date_trunc(tran_at, {{ date_grain }}),outlet_code) as {{ date_grain }}ly_quantity,
        {% endfor %}

        {{ dbt_utils.surrogate_key(['tran_at','outlet_code','sku_code']) }} as unique_id

    from orders
)

select * from aggregated
