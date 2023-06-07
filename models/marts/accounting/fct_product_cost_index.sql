with product_cog as (
    select *
    from {{ ref ('stg_ipos_accounting__sale_details') }}
    where product_id = item_id
        and not regexp_contains(account_id_cost, '^152|153')
        and account_id_cost != ''
),

sku_mapping as (
    select * from {{ ref('stg_sku_items') }}
),

bis_code as (
    select * from {{ ref('stg_gg_sheet__bis_code') }}
),

monthly_cog as (
    select

        date_trunc(product_cog.tran_at, month) as tran_month,

        product_cog.product_id,
        product_cog.item_id,
        case
            when upper(product_cog.warehouse_id) in (
                    'HCM-CK-PACC',
                    'HCM-CK1-BC',
                    'HCM-WH1-PACC',
                    'BEP'
                ) then 'HCM-WH1-PACC'
            else upper(product_cog.warehouse_id)
        end as warehouse_id,

        sum(product_cog.cog_amount_orig) as cog_amount_orig,
        sum(product_cog.quantity) as quantity

    from product_cog

    {{ dbt_utils.group_by(n=4) }}
)

select
    {{ dbt_utils.surrogate_key(['monthly_cog.tran_month','monthly_cog.product_id','monthly_cog.warehouse_id']) }} as unique_id,
    sku_mapping.item_category,
    sku_mapping.item_group,
    sku_mapping.item_name,
    lower(sku_mapping.unit_id) as unit_id,

    monthly_cog.*,
    lag(monthly_cog.cog_amount_orig,1,0) over (partition by monthly_cog.product_id, monthly_cog.warehouse_id order by monthly_cog.tran_month) as last_month_cog_amount_orig,
    lag(monthly_cog.quantity,1,0) over (partition by monthly_cog.product_id, monthly_cog.warehouse_id order by monthly_cog.tran_month) as last_month_quantity,
    lead(monthly_cog.cog_amount_orig,1,0) over (partition by monthly_cog.product_id, monthly_cog.warehouse_id order by monthly_cog.tran_month) as next_month_cog_amount_orig,
    lead(monthly_cog.quantity,1,0) over (partition by monthly_cog.product_id, monthly_cog.warehouse_id order by monthly_cog.tran_month) as next_month_quantity,

    bis_code.department_code,
    bis_code.function_code,
    bis_code.department_name,
    bis_code.bu_1 as bu,
    bis_code.bu_2,
    bis_code.bu_3,
    bis_code.zone,
    bis_code.owner,
    bis_code.accessed_email,
    bis_code.district,
    bis_code.function_name,
    bis_code.post_code,
    bis_code.province,
    bis_code.director,
    bis_code.function_manager,
    bis_code.department_manager,
    bis_code.so_at
from monthly_cog
left join bis_code
    on monthly_cog.warehouse_id = upper(bis_code.outlet_code)
left join sku_mapping
    on sku_mapping.sku_code = monthly_cog.item_id
