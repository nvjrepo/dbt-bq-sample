{{
    config(
        enabled=false
    )
}}
with sources as  (
    select * from {{ ref('base_ipos_crm__member_ratings') }}
),

renamed as (
    select
        id,
        membership_id,
        --member_id,
        tran_id,

        --date
        cast(created_at as datetime) as created_at,
        cast(update_at as datetime) as update_at,
        cast(expired_at as datetime) as expired_at,
        loaded_date as loaded_at,

        score,

        --reason
        reason_bad_food,
        reason_expensive_price,
        reason_bad_service,
        reason_bad_shipper,
        reason_other,
        reason_note,

        --PIC
        take_care_via,
        take_care_by_id,
        take_care_by_name,
        take_care_message,

        --other information
        published,
        source_fb_id,
        pos_id,
        pos_parent,
        order_code

    from sources

)

select * from renamed
