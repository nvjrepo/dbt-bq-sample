with sources as  (
    select * from {{ source('google_sheets','bis_code') }}
),

renamed as (
    select
        --code--
        function_code,
        department_code,
        coalesce(outlet_code,department_code) as outlet_code,
        
        --name--
        function_name,
        department_name,
        coalesce(outlet_name, department_name) as outlet_name,

        --level--
        bu_1,
        bu_2,
        bu_3,

        --address--
        zone,
        owner,
        op_email,
        ho_email,
        concat(coalesce(op_email,''),', ',coalesce(ho_email,'')) as accessed_email,
        district,
        postcode as post_code,
        province,

        --PIC--
        director,
        function_manager,
        department_manager,

        cast(so_date as timestamp) as so_at
    
    from sources
)

select * from renamed


