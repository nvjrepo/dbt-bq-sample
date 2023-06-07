with sources as (
    select * from {{ source('google_sheets','vas_chart_of_account') }}
),

renamed as (
    select
        --account--
        account_level_1,
        account_level_2,

        --description--
        group_1 as general_name,
        group_2 as name_level_1,
        group_3 as name_level_2

    from sources
)

select * from renamed
