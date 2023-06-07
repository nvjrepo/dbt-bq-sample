{%- set convert_metrics = ['gross_sales',
                           'cogs',
                           'item_price',
                           'promotion',
                           'service_charge',
                           'delivery_fee',
                           'item_discount',
                           'member_discount',
                           'discount',                     
                           'net_sales',
                           'gross_margin',
]-%}

with sale_details as  (
    select * from {{ ref('int_sale_detail_outlets') }}
),

sales as (
    select * from {{ ref('int_sales') }}
),

bis_code as (
    select * from {{ ref('stg_gg_sheet__bis_code') }}
),

joined as (
    select
        sale_details.*,
        sales.promotion*sale_details.divider as promotion,
        sales.service_charge*sale_details.divider as service_charge,
        sales.delivery_fee*sale_details.divider as delivery_fee,
        sale_details.gross_sales*sales.member_discount_pecent as member_discount

    from sale_details
    left join sales
        on sale_details.unique_sale_id = sales.unique_id
       
),

logics as (
    select
        *,
        member_discount+item_discount as discount,
        (
            gross_sales
            -- amount_vat
            - promotion
            - item_discount
            - member_discount
            + service_charge
            + delivery_fee
        )
        as net_sales,
        quantity*cog_unit_price_org as cogs,

        (
            gross_sales
            -- amount_vat
            - promotion
            - item_discount
            - member_discount
            + service_charge
            + delivery_fee
        )
        - (quantity*cog_unit_price_org) as gross_margin,
        case 
            when date_diff(current_timestamp(), cog_updated_at, day) <= 180 then 'new'
            else 'old'
        end as new_old


    from joined
),

final as (
    select
        logics.unique_id,
        logics.unique_sale_id,
        logics.outlet_code,
        logics.sku_code,
        logics.item_name,
        logics.tran_at,
        logics.quantity,
        {% for metric in convert_metrics -%}
            round(logics.{{ metric }}/1000,0) as {{ metric }},
        {% endfor -%}

        logics.new_old,
        logics.category,
        logics.item_group,
        logics.cog_updated_at,
        logics.week_day,
        logics.time_in,
        logics.time_out,
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
    
    from logics
    left join bis_code
        on logics.outlet_code = bis_code.outlet_code

)

select * from final



