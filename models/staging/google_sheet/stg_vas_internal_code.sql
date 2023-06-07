with sources as (
    select * from {{ source('google_sheets','vas_management_code') }}
),

renamed as (
    select
        {{ dbt_utils.surrogate_key(['vas_code','internal_code']) }} as unique_id,

        vas_code,
        vas_english,
        vas_vietnamese,

        internal_code,
        internal_english,
        internal_vietnamese,

        pl_1_english,
        pl_1_vietnamese,
        pl_2_vietnamese,
        pl_2_english,

        note,
        description

    from sources
)

select * from renamed
