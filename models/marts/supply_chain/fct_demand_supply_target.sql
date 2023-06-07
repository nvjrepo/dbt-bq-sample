{%- set standard_case = 7.92 -%}

with demand_supply as (
    select * from {{ ref('int_demand_supply') }}
    where extract(year from tran_day) >= 2022
),

targets as (
    select
        *,
        case when metric_names = 'abi_vol_outlet' then 'ABI' end as supplier

    from {{ ref('int_targets') }} 
    where metric_names = 'abi_vol_outlet'
        and metric_value != 0
),

util_days as (
    select * from {{ ref('util_days')}}
    where extract(year from date_day) >= 2022
),

item_mapping as (
    select * from {{ ref('stg_item_mapping') }}
),

bis_code as (
    select * from {{ ref('stg_gg_sheet__bis_code') }}
),

grouped as (
    select
        demand_supply.outlet_code,
        demand_supply.tran_day,
        item_mapping.supplier,
        sum(demand_supply.actual_supply_quantity_no_internal*item_mapping.litre_convert)/{{ standard_case }} as actual_supply_quantity_converted,
        sum(demand_supply.actual_demand_quantity*item_mapping.litre_convert)/{{ standard_case }} as actual_demand_quantity_converted     
    
    from demand_supply
    inner join item_mapping
        on demand_supply.item_id = item_mapping.item_id
    {{ dbt_utils.group_by(n=3) }}

),

joined as (
    select
        coalesce(util_days.date_day, targets.date_day) as tran_day,
        coalesce(grouped.outlet_code, targets.outlet_code) as outlet_code,
        coalesce(grouped.supplier, targets.supplier) as supplier,
        grouped.actual_supply_quantity_converted,
        grouped.actual_demand_quantity_converted,
        targets.metric_value as target_demand_quantity

    from util_days
    left join grouped
        on util_days.date_day = date(grouped.tran_day)
    full join targets
        on grouped.outlet_code = targets.outlet_code
            and util_days.date_day = targets.date_day
            and grouped.supplier = targets.supplier
),

final as (
    select
        joined.*,
        bis_code.bu_1 as bu,
        bis_code.accessed_email,
        {{ dbt_utils.surrogate_key(['joined.tran_day','joined.outlet_code','joined.supplier']) }} as unique_id

    from joined
    left join bis_code
        on joined.outlet_code = bis_code.outlet_code
)

select * from final
