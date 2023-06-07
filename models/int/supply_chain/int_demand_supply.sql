with acc_warehouses as (
    select * from {{ ref('stg_ipos_accounting__warehouses') }}
),

bom as (
    select
        sku_code,
        item_id,
        max(quantity) as quantity

    from {{ ref('stg_ipos_accounting__bom') }}
    {{ dbt_utils.group_by(n=2) }}
),

sale_details_quantity as (
    select
        date_trunc(tran_at,day) as tran_day,
        outlet_code,
        sku_code,
        sum(quantity) as selling_quantity

    from {{ ref('stg_ipos_sale_details') }}
    where extract(year from tran_at) > 2020
    {{ dbt_utils.group_by(n=3) }}
),

actual_supply_metric as (
    select
        date_trunc(tran_at,day) as tran_day,
        outlet_code,
        item_id,
        sum(quantity) as actual_supply_quantity,
        sum(case when tran_id != 'XCK' then quantity end) as actual_supply_quantity_no_internal

    from acc_warehouses
    where true
        and (tran_id like 'N%'
            or (tran_id = 'XCK'
                and issue_receive = 'N'
            )
        )
    {{ dbt_utils.group_by(n=3) }}
),

demand_quantity_metric as (
    select
        sale_details_quantity.tran_day,
        sale_details_quantity.outlet_code,
        bom.item_id,
        sum(sale_details_quantity.selling_quantity * bom.quantity) as actual_demand_quantity

    from sale_details_quantity
    left join bom
        on sale_details_quantity.sku_code = bom.sku_code
    {{ dbt_utils.group_by(n=3) }}

),

joined as (
    select
        coalesce(actual_supply_metric.outlet_code,demand_quantity_metric.outlet_code) as outlet_code,
        coalesce(actual_supply_metric.item_id,demand_quantity_metric.item_id) as item_id,
        coalesce(actual_supply_metric.tran_day,demand_quantity_metric.tran_day) as tran_day,
        sum(coalesce(actual_supply_metric.actual_supply_quantity_no_internal,0)) as actual_supply_quantity_no_internal,
        sum(coalesce(actual_supply_metric.actual_supply_quantity,0)) as actual_supply_quantity,
        sum(coalesce(demand_quantity_metric.actual_demand_quantity,0)) as actual_demand_quantity

    from actual_supply_metric
    full join demand_quantity_metric
        on actual_supply_metric.outlet_code = demand_quantity_metric.outlet_code
            and actual_supply_metric.item_id = demand_quantity_metric.item_id
            and actual_supply_metric.tran_day = demand_quantity_metric.tran_day
    {{ dbt_utils.group_by(n=3) }}

)

select * from joined
