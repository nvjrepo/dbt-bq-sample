with sources as (
    select * from {{ ref('acc_bom__unioned') }}
),

renamed as (
    select
        --pr, fr key
        cast(pr_key as int) as pr_key,

        --id
        job_id,
        item_id,
        product_id as sku_code,
        organization_id,

        --timetsamp
        safe.parse_timestamp('%FT%H:%M:%E*S', valid_date) as valid_until,

        --metric
        quantity,

        --system		
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_ca_bom_hashid

    from sources

)

select * from renamed
