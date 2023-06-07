with sources as (
    select * from {{ ref('base_ggsheet__target_sale_unioned') }}
),

renamed as (
    select
        {{ dbt_utils.surrogate_key(['branch_picker','forecasted_date']) }} as unique_id,
        branch_picker as outlet_code,

        parse_date("%a %b %e %Y", forecasted_date) as forecasted_date,
        parse_date("%a %b %e %Y", historical_date) as historical_date,

        {{ convert_string_to_number('sale_index') }} as sale_index,
        {{ convert_string_to_number('growth_rate') }} as growth_rate,
        {{ convert_string_to_number('historical_net_sales') }} as historical_net_sales,
        {{ convert_string_to_number('forecasted_net_sales') }} as forecasted_net_sales,

        loaded_date as loaded_date

    from sources
)

select * from renamed
