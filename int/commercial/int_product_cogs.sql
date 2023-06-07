with bom as (
    select
        sku_code, 
        item_id,
        max(valid_until) as valid_until,
        max(quantity) as quantity
  
    from {{ ref('stg_ipos_accounting__bom') }}
    {{ dbt_utils.group_by(n=2) }}
),

rm_price as (
    select * from {{ ref('int_rm_pricing_index') }}
),

item_grouped as (
    select
        item_id,
        max(day_start_at) as update_at

    from {{ ref('stg_ipos_accounting__sale_base_price') }} 
    group by 1
),

final as (
    select
        rm_price.date_month,
        bom.sku_code,
        sum(rm_price.unit_price * bom.quantity) as cog_unit_price,
        max(coalesce(item_grouped.update_at, bom.valid_until)) as updated_at
        
    from bom
    left join item_grouped
        on bom.item_id = item_grouped.item_id
    left join rm_price
        on bom.item_id = rm_price.item_id
    {{ dbt_utils.group_by(n=2) }}
    having cog_unit_price is not null

)

select * from final
