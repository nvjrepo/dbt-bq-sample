with sources as (
    select * from {{ source('ipos_sale','sale_detail') }}
),

renamed as (
    select distinct
        {{ dbt_utils.surrogate_key(['fr_key','pr_key','workstation_name']) }} as unique_id,
        {{ dbt_utils.surrogate_key(['fr_key','workstation_name']) }} as unique_sale_id,

        --key id transaction
        cast(fr_key as string) as sale_id,
        cast(pr_key as string) as sale_detail_id,
        cast(pr_key_order as string) as pr_key_order,

        user_id,
        --others id
        item_id as sku_code,
        item_id_mapping,
        package_id,
        promotion_id,
        parent_item_id,

        unit_id,
        cast(workstation_id as string) as workstation_id,
        membership_id,

        --time 
        --timestamp(datetime(cast(sale_date as datetime), "Asia/Bangkok")) sale_date,
        --timestamp(datetime(cast(end_date as datetime),  "Asia/Bangkok")) end_date,
        timestamp(tran_date) as tran_at,
        hour_start,
        hour_end,
        minute_start,
        minute_end,

        --metric
        tax,
        amount,
        cast(number as float64) as number,
        price_sale,
        price_org,
        cost_price,
        amount_point,
        discount as discount_percent,
        quantity,
        temp_calculate,
        quantity_at_temp,

        --bolean
        is_fc = 1 as is_fc,
        is_kit = 1 as is_kit,
        is_set = 1 as is_set,
        is_gift = 1 as is_gift,
        is_invoice = 1 as is_invoice,
        is_service = 1 as is_service,
        is_eat_with = 1 as is_eat_with,
        is_print_label = 1 as is_print_label,

        --info
        workstation_name as outlet_code,
        description as item_name,
        payment_type,
        cast(payment as string) as payment,

        --others
        fix,
        note,
        ots_ta,
        printed,
        list_order,
        stop_service,
        printed_label,

        --logic
        cast(concat(hour_start, ':', minute_start, ':00') as time) as time_in,
        cast(concat(hour_end, ':', minute_end, ':00') as time) as time_out

        ---workstation_name

    from sources
    where lower(workstation_name) not like '%test%'
    qualify row_number() over(partition by cast(pr_key as string) order by sale_date desc) = 1

)

select * from renamed
