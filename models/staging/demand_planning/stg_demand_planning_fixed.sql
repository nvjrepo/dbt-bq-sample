
with sources as  (
    select * from {{ ref('demand_planning__unioned') }}
),

renamed as (
    select
        {{ dbt_utils.surrogate_key(['date_month','outlet_code','item_id']) }} as unique_id,
        --code--
        cast(date_month as date) as date_month,
        outlet_code,	
        item_id,	
        cast(`Months` as float64) as demand_forecast
    
    from sources
    where date_month is not null
)

select * from renamed





