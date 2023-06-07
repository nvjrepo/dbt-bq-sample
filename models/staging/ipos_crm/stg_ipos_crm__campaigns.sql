with sources as (
    select * from {{ ref ('base_ipos_crm__campaigns') }}
),

renamed as (
    select
        id as campaign_id,
        many_times_code as voucher_id,
        list_pos_id,
        pos_parent,
        pos_id,
        cast(discount_type as string) as discount_type_id,
        cast(campaign_type as string) as campaign_type_id,
        preferential_type as preferential_type_id,

        --discount
        cast(discount_amount as float64) as discount_amount,
        cast(discount_extra as float64) as discount_extra,
        cast(comm_rate as float64) as comm_rate,
        cast(comm_amount as float64) as comm_amount,
        cast(comm_max as float64) as comm_max,
        cast(mkt_rate as float64) as mkt_rate,
        cast(mkt_amount as float64) as mkt_amount,
        cast(mkt_max as float64) as mkt_max,

        --campaign information
        voucher_campaign_name,
        case when pos_parent = 'BGN' then 'BAGAC' else 'FW' end as bu,
        campaign_type,
        duration,
        limit_discount_item,
        apply_type as appy_type_id,

        --manager
        manager_id,
        manager_name,

        --affiliate
        affiliate_id,
        affiliate_discount_type,
        cast(affiliate_discount_amount as float64) as affiliate_discount_amount,
        cast(affiliate_discount_extra as float64) as affiliate_discount_extra,

        --delivery
        delivery_discount_type,
        delivery_discount_value,

        --sku item
        item_type_id_list,
        item_id_list,
        apply_item_id_list,
        apply_item_type_id_list,
        json_gift_items,
        list_item_up_size,

        --metrics
        cast(quantity_per_day as float64) as quantity_per_day,
        cast(amount_order_over as float64) as amount_order_over,
        cast(accumulated_amount as float64) as accumulated_amount,
        cast(discount_max as float64) as discount_max,
        cast(min_quantity_discount as int64) as min_quantity_discount,
        cast(max_quantity_log as float64) as max_quantity_log,
        cast(number_of_buy_item as float64) as number_of_buy_item,
        cast(number_of_free_item as float64) as number_of_free_item,
        cast(same_price as float64) as same_price,

        --statistics
        cast(total_publish as float64) as number_of_published_vouchers,
        cast(total_used as float64) as number_of_used_vouchers,
        cast(total_used_amount as float64) as aggregated_used_amount,
        cast(total_discount_amount as float64) as aggregated_discount_amount,

        sms_content,

        --exchange
        cast(exchange_point_rate as float64) as exchange_point_rate,
        cast(exchange_point as float64) as exchange_point,

        --boolean
        cast(is_discount_combo as int64) = 1 as is_discount_combo,
        cast(is_same_item as int64) = 1 as is_same_item,
        cast(is_coupon as int64) = 1 as is_coupon,
        cast(is_delivery as int64) = 1 as is_delivery,
        cast(is_split_discount_per_item as int64) = 1 as is_split_discount_per_item,
        cast(is_ots as int64) = 1 as is_ots,
        cast(is_all_item as int64) = 1 as is_all_item,
        cast(active as int64) = 1 as is_active,
        cast(requied_member as int64) = 1 as is_requied_member,
        cast(discount_one_item as int64) = 1 as is_discount_one_item,
        cast(lucky_number as int64) = 1 as is_lucky_number,
        cast(only_coupon as int64) = 1 as is_only_coupon,
        cast(apply_once_per_user as int64) = 1 as is_apply_once_per_user,
        cast(only_buyer_can_use as int64) = 1 as is_only_buyer_can_use,
        cast(is_momo_campaign as int64) = 1 as is_momo_campaign,

        --datetime
        cast(date_created as datetime) as created_at,
        cast(date_updated as datetime) as updated_at,
        cast(date_start as datetime) as started_at,
        cast(date_end as datetime) as ended_at,

        time_hour_day,
        time_date_week,

        --metadata
        _dbt_source_relation,
        offset_time,
        apply_source,
        loaded_date as loaded_at

    from sources
    qualify row_number() over (partition by campaign_id order by loaded_date desc) = 1

)

select *
from renamed
where is_active
qualify row_number() over (partition by campaign_id, created_at, pos_parent order by updated_at desc) = 1
