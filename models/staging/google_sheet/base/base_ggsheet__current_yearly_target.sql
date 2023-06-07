with sources as (
    select * from {{ source('google_sheets','yearly_target') }}
),

renamed as (
    select
        {{ dbt_utils.surrogate_key(['string_field_0','string_field_1','extract(year from current_date())']) }} as unique_id,
        string_field_0 as metric_names,
        string_field_1 as outlet_code,
        extract(year from current_date()) as target_year,
        {{ convert_string_to_number('string_field_12') }} as m_1,
        {{ convert_string_to_number('string_field_13') }} as m_2,
        {{ convert_string_to_number('string_field_14') }} as m_3,
        {{ convert_string_to_number('string_field_15') }} as m_4,
        {{ convert_string_to_number('string_field_16') }} as m_5,
        {{ convert_string_to_number('string_field_17') }} as m_6,
        {{ convert_string_to_number('string_field_18') }} as m_7,
        {{ convert_string_to_number('string_field_19') }} as m_8,
        {{ convert_string_to_number('string_field_20') }} as m_9,
        {{ convert_string_to_number('string_field_21') }} as m_10,
        {{ convert_string_to_number('string_field_22') }} as m_11,
        {{ convert_string_to_number('string_field_23') }} as m_12
        --string_field_2 as outlet_name,
        --string_field_3 as post_code,
        --string_field_4 as province,
        --string_field_5 as outlet_zone,
        --string_field_6 as outlet_owner,
        --string_field_7 as am,
        --string_field_8 as bu3,
        --string_field_9 as bu2,
        --string_field_10 as monthly_sale_target,
        --string_field_11 as ytd_target,
        --string_field_24 as fy_target

    from sources
    where string_field_0 is not null and string_field_0 != 'metrics'
)

select * from renamed
