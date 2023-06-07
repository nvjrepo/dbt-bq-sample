with materials as (
    select * from {{ ref('stg_ipos_accounting__warehouses') }}
    where true
        and issue_receive = 'N'
        and not regexp_contains(account_id, '^155|156')
),

item_mapping as (
    select
        item_id,
        case
            when item_group = 'BTP' then item_group
            else item_category
        end as item_category,

        case
            when item_group = 'BTP' then item_group1
            else item_group
        end as item_group
    from {{ ref('stg_item_mapping') }}
),

item_dim as (
    select *
    from {{ ref('stg_ipos_accounting__raw_materials') }}
),

bis_code as (
    select * from {{ ref('stg_gg_sheet__bis_code') }}
),

monthly_cog as (

    select
        date_trunc(materials.tran_at, month) as tran_month,

        -- materials.product_id,
        materials.item_id,
        case
            when upper(materials.warehouse_id) in (
                    'HCM-CK-PACC',
                    'HCM-CK1-BC',
                    'HCM-WH1-PACC',
                    'BEP'
                ) then 'HCM-WH1-PACC'
            else upper(materials.warehouse_id)
        end as warehouse_id,

        sum(materials.quantity * materials.unit_price) as cog_amount_orig,
        sum(materials.quantity) as quantity

    from materials

    {{ dbt_utils.group_by(n=3) }}
)

select
    {{ dbt_utils.surrogate_key(['monthly_cog.tran_month','monthly_cog.item_id','monthly_cog.warehouse_id']) }} as unique_id,

    item_mapping.item_category,
    item_mapping.item_group,
    lower(item_dim.item_name) as item_name,
    lower(item_dim.unit_id) as unit_id,

    monthly_cog.*,
    lag(monthly_cog.cog_amount_orig,1,0) over (partition by monthly_cog.item_id,monthly_cog.warehouse_id order by monthly_cog.tran_month) as last_month_cog_amount_orig,
    lag(monthly_cog.quantity,1,0) over (partition by monthly_cog.item_id,monthly_cog.warehouse_id order by monthly_cog.tran_month) as last_month_quantity,
    lead(monthly_cog.cog_amount_orig,1,0) over (partition by monthly_cog.item_id,monthly_cog.warehouse_id order by monthly_cog.tran_month) as next_month_cog_amount_orig,
    lead(monthly_cog.quantity,1,0) over (partition by monthly_cog.item_id,monthly_cog.warehouse_id order by monthly_cog.tran_month) as next_month_quantity,

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
left join item_mapping
    on monthly_cog.item_id = upper(item_mapping.item_id)
left join item_dim
    on monthly_cog.item_id = upper(item_dim.item_id)
