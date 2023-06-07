with sources as (
    select * from {{ ref('base_ipos_crm__member_vouchers') }}
),

renamed as (
    select
        --id
        voucher_code,
        voucher_campaign_id,
        membership_id,
        item_type_id_list,
        apply_item_id,
        list_pos_id,
        item_id_list,

        --voucher
        voucher_campaign_name,
        voucher_description,

        --pos
        pos_parent,
        pos_id,
        dm_pos_parent,
        list_pos,

        --date
        timestamp(date_created) as created_at,
        timestamp(date_start) as started_at,
        timestamp(date_end) as ended_at,
        case when date_hash = '' then datetime(null) else cast(date_hash as datetime) end as hashed_at,
        case when used_date = 'nan' then datetime(null) else cast(used_date as datetime) end as used_at,
        loaded_date as loaded_at,
        time_hour_day,
        time_date_week,

        --discount
        discount_type,
        discount_amount,
        discount_extra,
        discount_per_item,
        discount_one_item,
        discount_max,
        limit_discount_item,
        min_quantity_discount,

        --other metrics
        requied_member as number_of_member_required,
        amount_order_over,
        number_item_buy as number_of_item_buy,
        number_item_free as number_of_item_free,

        --affiliate
        affiliate_id,
        affiliate_discount_type,
        affiliate_discount_amount,
        affiliate_discount_extra,
        affiliate_used_total_amount,

        --used
        used_sale_tran_id,
        used_discount_amount,
        used_bill_amount,
        used_member_info,
        used_pos_id,

        --boolean
        is_all_item = 1 as is_all_item,
        is_delivery = 1 as is_delivery,
        is_ots = 1 as is_ots,
        is_coupon = 1 as is_coupon,
        only_coupon = 1 as is_only_coupon,
        has_sale_manager = 1 as has_sale_manager,
        same_price = 1 as is_same_price,

        --other information
        status as voucher_status,
        buyer_info,
        apply_item_type,
        preferential_type

    from sources
    qualify row_number() over (partition by voucher_code order by loaded_date desc) = 1

)

select * from renamed
