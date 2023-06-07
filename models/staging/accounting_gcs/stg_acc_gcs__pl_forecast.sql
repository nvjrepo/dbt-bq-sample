with sources as  (
    select * from {{ source('accounting_gcs','pl_forecast') }}
),

renamed as (
    select
        {{ dbt_utils.surrogate_key(['expense_id','forecast_year']) }} as unique_id,
        expense_id,
        pl_section,
        forecast_year,
        m_1,
        m_2,
        m_3,
        m_4,
        m_5,
        m_6,
        m_7,
        m_8,
        m_9,
        m_10,
        m_11,
        m_12
    
    from sources
    where safe_cast(expense_id as int64) is not null
)

select * from renamed
