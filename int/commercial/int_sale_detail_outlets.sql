with sale_details as  (
    select * from {{ ref('stg_ipos_sale_details') }}
),

sku_mapping as (
    select * from {{ ref('stg_sku_items') }}
),

product_cogs as (
    select * from {{ ref('int_product_cogs') }}
),

joined as (
    select
        sale_details.*,
        format_date('%a', sale_details.tran_at) as week_day,
        sku_mapping.item_category as category,
        sku_mapping.item_group,
        coalesce(product_cogs.cog_unit_price,0) as cog_unit_price,
        coalesce(product_cogs.cog_unit_price,0) as cog_unit_price_org,
        product_cogs.updated_at as cog_updated_at,
        case
            when outlet_code ='HCM-BG1-SH' then
                case 
                    when lower(sku_mapping.item_category)='beverage' then sale_details.price_sale/1.1
                    else sale_details.price_sale/1.08
                end
            else sale_details.price_sale
        end as item_price

    from sale_details
    left join sku_mapping
        on sale_details.sku_code = sku_mapping.sku_code
    left join product_cogs
        on sale_details.sku_code = product_cogs.sku_code
            and date_trunc(date(sale_details.tran_at), month) = product_cogs.date_month

),

final as (
    select
        *,
        item_price*quantity as gross_sales,
        item_price*quantity*discount_percent as item_discount,
        item_price*quantity*tax as amount_vat,
        coalesce(safe_divide(item_price*quantity,sum(item_price*quantity) over (partition by unique_sale_id)),0) as divider,


    from joined
)

select * from final
