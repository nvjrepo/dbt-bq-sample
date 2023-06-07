with sources as  (
    select * from {{ source('accounting_gcs','cashflow_forecast') }}
),

renamed as (
    select
        {{ dbt_utils.surrogate_key(['forecast_year','cf_code']) }} as unique_id,
        `no` as section_order,
        cf_code,
        cashflow_from_business_activities as cf_name,
        forecast_year,
        {{ convert_string_to_number('m_1') }} as m_1,
        {{ convert_string_to_number('m_2') }} as m_2,
        {{ convert_string_to_number('m_3') }} as m_3,
        {{ convert_string_to_number('m_4') }} as m_4,
        {{ convert_string_to_number('m_5') }} as m_5,
        {{ convert_string_to_number('m_6') }} as m_6,
        {{ convert_string_to_number('m_7') }} as m_7,
        {{ convert_string_to_number('m_8') }} as m_8,
        {{ convert_string_to_number('m_9') }} as m_9,
        {{ convert_string_to_number('m_10') }} as m_10,
        {{ convert_string_to_number('m_11') }} as m_11,
        {{ convert_string_to_number('m_12') }} as m_12
    
    from sources
    where cf_code is not null
)

select * from renamed
