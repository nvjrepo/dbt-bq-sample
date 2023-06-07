with demand_supply as (
    select * from {{ ref('int_demand_supply') }}
    where extract(year from tran_day) >= 2022
),

bis_code as (
    select * from {{ ref('stg_gg_sheet__bis_code') }}
),

item_mapping as (
    select * from {{ ref('stg_item_mapping') }}
),

pricing_index as (
    select * from {{ ref('int_rm_pricing_index') }}
),

movement as (
    select
        date_trunc(date(tran_day),month) as tran_month,
        outlet_code,
        item_id,
        sum(actual_supply_quantity) as actual_supply_quantity,
        sum(actual_demand_quantity) as actual_demand_quantity

    from demand_supply
    {{ dbt_utils.group_by(n=3) }}
),

warehouse_balances as (
    select
        *,
        date_add(date_trunc(date(tran_at),month), interval 1 month) as tran_month
    from {{ ref('stg_ipos_accounting__warehouse_balances') }}
    where date(tran_at) >= date '2021-12-01'
),

joined as (
    select
        coalesce(warehouse_balances.tran_month,movement.tran_month) as tran_month,
        coalesce(warehouse_balances.warehouse_id,movement.outlet_code) as outlet_code,
        coalesce(warehouse_balances.item_id,movement.item_id) as item_id,
        sum(coalesce(warehouse_balances.quantity,0)) as beginning_stock_quantity,
        sum(coalesce(movement.actual_supply_quantity,0)) as actual_supply_quantity,
        sum(coalesce(movement.actual_demand_quantity,0)) as actual_demand_quantity,
        sum(coalesce(warehouse_balances.quantity,0)
            + coalesce(movement.actual_supply_quantity,0)
            - coalesce(movement.actual_demand_quantity,0)
        ) as ending_stock_quantity

    from warehouse_balances
    full join movement
        on warehouse_balances.tran_month = movement.tran_month
            and warehouse_balances.warehouse_id = movement.outlet_code
            and warehouse_balances.item_id = movement.item_id
    where coalesce(warehouse_balances.quantity,0) + coalesce(movement.actual_supply_quantity,0) + coalesce(movement.actual_demand_quantity,0) != 0
    {{ dbt_utils.group_by(n=3) }}
),

final as (
    select
        {{ dbt_utils.surrogate_key(['joined.tran_month','joined.outlet_code','joined.item_id']) }} as unique_id,
        joined.*,
        item_mapping.item_category,
        item_mapping.item_group,
        item_mapping.item_group1,
        item_mapping.supplier,
        pricing_index.item_name,
        pricing_index.unit_price,
        bis_code.bu_1 as bu,
        bis_code.accessed_email

    from joined
    left join bis_code
        on joined.outlet_code = bis_code.outlet_code

    left join item_mapping
        on joined.item_id = item_mapping.item_id

    left join pricing_index
        on joined.item_id = pricing_index.item_id
            and date(joined.tran_month) = pricing_index.date_month
)

select * from final
