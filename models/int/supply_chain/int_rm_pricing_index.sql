{%- set orders =['asc','desc'] -%}

with items as (
    select distinct
        item_id,
        item_name
    from {{ ref('stg_ipos_accounting__raw_materials') }}
--where lower(item_class_id) in ('btp','nvl','hh')
),

movement_warehouse as (
    select
        date_trunc(tran_at,month) as tran_month,
        item_id,
        avg(unit_price) as unit_price
    from {{ ref('stg_ipos_accounting__warehouses') }}
    where unit_price != 0
    {{ dbt_utils.group_by(n=2) }}
),

movement_cost as (
    select
        date_trunc(tran_at,month) as tran_month,
        item_id,
        avg(unit_price) as unit_price
    from {{ ref('stg_ipos_accounting__sale_details') }}
    where unit_price != 0
    {{ dbt_utils.group_by(n=2) }}
),

util_month as (
    select * from {{ ref('util_months') }}
    where date_month > '2018-01-01'
),

item_monthly_index as (
    select
        util_month.date_month,
        items.item_id,
        items.item_name
    from util_month
    cross join items
),

joined as (
    select
        item_monthly_index.*,
        coalesce(movement_warehouse.unit_price,movement_cost.unit_price) as unit_price,
        row_number() over (order by item_monthly_index.date_month) as rn

    from item_monthly_index
    left join movement_warehouse
        on item_monthly_index.date_month = date(movement_warehouse.tran_month)
            and item_monthly_index.item_id = movement_warehouse.item_id
    left join movement_cost
        on item_monthly_index.date_month = date(movement_cost.tran_month)
            and item_monthly_index.item_id = movement_cost.item_id

),

row_group_order as (
    select
        *,
        {% for order in orders -%}
            sum(case when unit_price is null then 0 else 1 end)
            over (partition by item_id order by rn {{ order }})
            as row_group_{{ order }}{{ ',' if not loop.last }}
        {% endfor -%}
    from joined
),

null_month_filled as (
    select
        *,
        {% for order in orders -%}
            coalesce(unit_price,
                 first_value(unit_price) over (partition by item_id,row_group_{{ order }} order by rn {{ order }})
        ) as unit_price_final_{{ order }}{{ ',' if not loop.last }}
        {% endfor -%}

  from row_group_order
),

final as (
    select
        date_month,
        item_id,
        item_name,
        coalesce(unit_price_final_asc,unit_price_final_desc) as unit_price

    from null_month_filled
    where coalesce(unit_price_final_asc,unit_price_final_desc) is not null
)

select * from final
