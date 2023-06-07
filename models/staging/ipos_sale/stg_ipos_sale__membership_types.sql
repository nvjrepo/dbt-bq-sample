with sources as  (
    select * from {{ source('ipos_sale','dm_membership_type') }}
),

renamed as (
    select
        membership_type_id,
        user_id,
        workstation_id,
        membership_type_parent_id,
        
        membership_type_name,
        point_to_amount,
        
        active=1 as is_active,
        is_once=1 as is_once

    from sources
    where membership_type_id != ''
    qualify row_number() over (partition by membership_type_id) = 1

)

select * from renamed
