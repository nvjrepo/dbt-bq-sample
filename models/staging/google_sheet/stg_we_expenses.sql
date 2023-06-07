{{
    config(
        enabled=false
    )
}}
with sources as  (
    select * from {{ source('google_sheet','we_Data') }}
),

renamed as (
    select
        {{ dbt_utils.surrogate_key(['outlet_code','event_at']) }} as unique_id,
        outlet_code,
        timestamp(right(event_at,4) || '-' || substring(event_at,4,2) || '-' || left(event_at,2)) as event_at,

        cast(replace(kw_low_index,',','.') as float64) as kw_low_index,
        cast(replace(kw_mid_index,',','.') as float64) as kw_mid_index,
        cast(replace(kw_high_index,',','.') as float64) as kw_high_index,
        cast(replace(kw_low_price,',','.') as float64) as kw_low_price,
        cast(replace(kw_mid_price,',','.') as float64) as kw_mid_price,
        cast(replace(kw_high_price,',','.') as float64) as kw_high_price,
        cast(replace(m3_water_price,',','.') as float64) as m3_water_price,
        cast(replace(m3_water_index,',','.') as float64) as m3_water_index,

        cast(replace(water_cost,',','.') as float64) as water_cost,
        cast(replace(electric_cost,',','.') as float64) as electric_cost,
        cast(replace(total_cost,',','.') as float64) as total_cost

        --year,
        --month,
        --weekday,

        --system--
        --_airbyte_ab_id,
        --_airbyte_emitted_at,
        --_airbyte_normalized_at,
        --_airbyte_we_Data_hashid
    
    from sources
)

select * from renamed
