with sources as (
    select * from {{ source('google_sheets','raw_material_mapping') }}
),

renamed as (
    select
        item_id,
        item_category,
        item_group,
        item_group1,
        supplier,
        cast(litre_convert as float64) as litre_convert


    from sources
)

select * from renamed
