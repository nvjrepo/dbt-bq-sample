with sources as  (
    select * from {{ source('ipos_sale','dm_extra_2') }}
),

renamed as (
    select
        cast(extra_id_2 as string) as party_id,	
        extra_name_2 as party_name,	
        active=1 as is_active,		
        user_id,	
        workstation_id,		
        payment_type,	
        commission_rate,			
        payment_method_id

    from sources
    qualify row_number() over (partition by extra_id_2) = 1

)

select * from renamed

--item_image,
--item_color,