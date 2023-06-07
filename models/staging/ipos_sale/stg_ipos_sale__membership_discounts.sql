with sources as  (
    select * from {{ source('ipos_sale','dm_membership_discount') }}
),

renamed as (
    select
        --id
        pr_key,
        membership_type_id,
        membership_type_parent_id,
        type_id,
        item_id,
        user_id,
        workstation_id,
        promotion_id,

        --timestamp
        timestamp(datetime(timestamp(from_date, "Asia/Bangkok"))) as started_at,
        timestamp(datetime(timestamp(end_date, "Asia/Bangkok"))) as ended_at,

        --percent discount
        ta_discount,
        ots_discount,
        point_to_amount,

        --other information
        ta_birthday,
        ots_birthday,
        data_source,
        membership_type_name,

        --boolean
        active=1 as is_active,
        is_all_week=1 as is_all_week,
        is_type=1 as is_type,
        is_item=1 as is_item,
        is_once=1 as is_once,
        is_all=1 as is_all,
        is_sun=1 as is_sun,
        is_mon=1 as is_mon,
        is_tue=1 as is_tue,
        is_wed=1 as is_wed,
        is_thu=1 as is_thu,
        is_fri=1 as is_fri,
        is_sat=1 as is_sat

    from sources
    where membership_type_id != ''

)

select * from renamed