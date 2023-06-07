{%- set standard_case = 7.92 -%}
with stock_balance_metric as (
    select * from {{ ref('int_warehouse') }}
),

demand_supply as (
    select * from {{ ref('int_demand_supply') }}
),

raw_materials as (
    select * from {{ ref('stg_ipos_accounting__raw_materials') }}
),

item_mapping as (
    select * from {{ ref('stg_item_mapping') }}
),

demand_planning_fixed as (
    select * from {{ ref('stg_demand_planning_fixed') }}
),

targets as (
    select * from {{ ref('int_year_target_unpivoted') }}
    where metric_names = 'abi_vol_outlet'
),

bis_code as (
    select * from {{ ref('stg_gg_sheet__bis_code') }}
),

final as (
    select
        {{ dbt_utils.surrogate_key(['demand_supply.tran_day','stock_balance_metric.outlet_code','stock_balance_metric.item_id']) }} as unique_id,
        stock_balance_metric.*,
        raw_materials.unit_id,
        raw_materials.item_name,
        item_mapping.item_category,
        item_mapping.item_group,
        item_mapping.item_group1,
        item_mapping.supplier,
        demand_supply.actual_demand_quantity,
        demand_supply.actual_supply_quantity,
        coalesce(targets.metric_value,0)
        / count(*) over (partition by targets.date_month, stock_balance_metric.outlet_code)
        as target_demand_quantity,
        demand_supply.tran_day,
        bis_code.bu_1 as bu,
        bis_code.accessed_email,
        demand_planning_fixed.demand_forecast
        / count(*) over (partition by demand_planning_fixed.date_month,stock_balance_metric.outlet_code,stock_balance_metric.item_id)
        as demand_forecast_final

    from stock_balance_metric

    left join demand_supply
        on stock_balance_metric.outlet_code = demand_supply.outlet_code
            and stock_balance_metric.item_id = demand_supply.item_id
    left join raw_materials
        on stock_balance_metric.item_id = raw_materials.item_id
    inner join item_mapping
        on stock_balance_metric.item_id = item_mapping.item_id
    left join demand_planning_fixed
        on stock_balance_metric.item_id = demand_planning_fixed.item_id
            and stock_balance_metric.outlet_code = demand_planning_fixed.outlet_code
            and date(date_trunc(demand_supply.tran_day,month)) = demand_planning_fixed.date_month
    left join targets
        on stock_balance_metric.outlet_code = targets.outlet_code
            and date(date_trunc(demand_supply.tran_day,month)) = targets.date_month
            and lower(item_mapping.supplier) = 'abi'
    left join bis_code
        on stock_balance_metric.outlet_code = bis_code.outlet_code

)

select * from final
