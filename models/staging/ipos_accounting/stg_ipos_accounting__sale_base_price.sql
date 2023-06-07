with sources as (
    select * from {{ source('accounting_all','SALE_BASE_PRICE_DETAIL_ANALYTICS') }}
),

renamed as (
    select
        --pr, fr key
        cast(pr_key as int) as pr_key,
        cast(fr_key as int) as fr_key,

        --id
        item_id,
        unit_id,
        currency_id,
        pr_detail_id,
        price_level_id,

        --timestamp
        parse_timestamp('%FT%H:%M:%E*S', day_start) as day_start_at,
        parse_timestamp('%FT%H:%M:%E*S', day_end) as day_end_at,

        -- metric number
        unit_price_orig,
        quantity_order_min,

        --addt information
        detail_active,
        list_order,
        is_fix,

        --system	
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_sale_base_price_detail_analytics_hashid

    from sources
    qualify row_number() over(partition by cast(pr_key as string) order by _airbyte_emitted_at desc) = 1

)

select * from renamed
