{%- set categories=['food',
                    'beverage',
                    'other'

]-%}

with order_items as  (
    select * from {{ ref('fct_order_line_items') }}
),

col_metric as (
    select
        outlet_code,
        timestamp(checkin_date) as tran_at,
        'col' as metric_names,
        sum(labour_cost)/1000 as metric_value

    from {{ ref('int_daily_col') }}
    {{ dbt_utils.group_by(n=3) }}
),

net_sale_metric as (
    select
        outlet_code,
        date_trunc(tran_at, day) as tran_at,
        'net_sales' as metric_names,
        sum(net_sales) as metric_value
    from order_items
    {{ dbt_utils.group_by(n=3) }}      
),

{% for category in categories -%}

net_sale_{{ category }}_metric as (
    select
        outlet_code,
        date_trunc(tran_at, day) as tran_at,
        concat ('net_sales_','{{ category }}') as metric_names,
        sum(net_sales) as metric_value
    from order_items
    where lower(category) = '{{ category }}'
    {{ dbt_utils.group_by(n=3) }}      
),

{% endfor %}

cog_metric as (
    select
        outlet_code,
        date_trunc(tran_at, day) as tran_at,
        'cog' as metric_names,
        sum(cogs) as metric_value
    from order_items
    {{ dbt_utils.group_by(n=3) }} 
),

gross_margin_metric as (
    select
        outlet_code,
        date_trunc(tran_at, day) as tran_at,
        'gross_margin' as metric_names,
        sum(gross_margin) as metric_value
    from order_items
    {{ dbt_utils.group_by(n=3) }} 
),

unioned as (
    select * from net_sale_metric
    union all
    select * from cog_metric
    union all
    select * from col_metric
    union all
    select * from gross_margin_metric
    {% for category in categories -%}
    union all
    select * from net_sale_{{ category }}_metric
    {% endfor %}

)

select * from unioned


--cog_percent_metric as (
--    select
--        outlet_code,
--        date_trunc(tran_at, day) as tran_at,
--        'cog_percent' as metric_names,
--        coalesce(sum(cogs)/sum(net_sales),0) as metric_value
--    from order_items
--    {{ dbt_utils.group_by(n=3) }} 
--),
