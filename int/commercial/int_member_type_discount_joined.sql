with membership_types as (
    select 
        membership_type_id,
        membership_type_name
    
    from {{ ref('stg_ipos_sale__membership_types') }}
),

membership_discounts as (
    select distinct
        membership_type_id,
        started_at,
        ta_discount,
        row_number() over (partition by membership_type_id order by started_at) as rn
        
    from {{ ref('stg_ipos_sale__membership_discounts') }}
    where is_active
    
),

member_discount_ended_date_map as (
    select
        membership_type_id,
        case when rn = 1 then timestamp('2016-01-01') else started_at end as started_at,
        coalesce(lead(started_at) over (partition by membership_type_id order by started_at),
            current_timestamp()
        ) as ended_at,
        ta_discount as member_discount_pecent

    from membership_discounts
),

joined as (
    select
        {{ dbt_utils.surrogate_key(['membership_types.membership_type_id','member_discount_ended_date_map.started_at']) }} as unique_id,
        membership_types.*,
        member_discount_ended_date_map.started_at,
        member_discount_ended_date_map.ended_at,
        member_discount_ended_date_map.member_discount_pecent

    from membership_types
    left join member_discount_ended_date_map
        on member_discount_ended_date_map.membership_type_id = membership_types.membership_type_id
            and member_discount_ended_date_map.started_at != member_discount_ended_date_map.ended_at
)

select * from joined
