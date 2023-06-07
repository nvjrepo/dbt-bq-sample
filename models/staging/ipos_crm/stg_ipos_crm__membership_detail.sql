{{ config(
  enabled=false
) }}

with sources as  (
    select * from {{ ref('base_ipos_crm__membership_details') }}
),

renamed as (
    select
        --id
        membership_id,
        id as unique_id,
        membership_id_new,

        --pos
        pos_parent,
        last_pos,
        last_pos_name,

        --date
        cast(first_eat_date as datetime) as first_eat_date,
        cast(last_eat_date as datetime) as last_eat_date,
        cast(created_at as datetime) as created_at,
        cast(update_at as datetime) as update_at,
        loaded_date	as loaded_at,

        --member
        name as member_name,
        phone_number,
        birthday,
        email,
        address as member_address,
        gender,
        user_groups,
        birth_month,
        age,

        --membership type
        membership_type_id,
        membership_type_name,
        cast(membership_type_change_at as datetime) as membership_type_change_at,
        
        --city
        city_id,
        city_name,

        --social media
        facebook_messenger_id,
        facebook_join_at,
        zalo_id,
        zalo_join_at,
        zalo_last_msg_id,
        cast(zalo_last_msg_at as datetime) as zalo_last_msg_at,
        is_zalo_follow=1 as is_zalo_follow,

        --numeric
        point as member_point,
        point_amount,
        payment_amount,
        eat_times as numer_eat_times,

        --other informatio
        ignore_marketing_message,
        created_by,
        tags

    from sources
    qualify row_number() over (partition by membership_id order by update_at desc) = 1

)

select * from renamed
