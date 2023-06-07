with sources as (
    select * from {{ ref('acc_warehouse_balance__unioned') }}
),

renamed as (
    select
        {{ dbt_utils.surrogate_key(['pr_key','warehouse_id']) }} as unique_id,

        -- primary key
        pr_key,

        -- id
        case
            when warehouse_id = 'BGHM' then 'HCM-BG1-SH'
            when warehouse_id = 'BGLQD' then 'HCM-BG2-LQD'
            when warehouse_id = 'BC' then 'HCM-BG3-BC'
            when warehouse_id = 'BGLVS' then 'HCM-BG4-LVS'
            when warehouse_id = 'BGUT' then 'HCM-BG5-UT'
            when warehouse_id = 'BGTS' then 'HCM-BG6-TS'
            when warehouse_id = 'BGNKKN' then 'HCM-BG7-NKKN'
            when warehouse_id = 'BGQT' then 'HCM-BG8-QT'
            when warehouse_id = '5WINE' then 'HCM-FW1-NKKN'
            else warehouse_id
        end as warehouse_id,
        job_id,

        upper(item_id) as item_id,
        unit_id_adj,
        account_id,
        account_id_adjust,
        user_id,
        organization_id,

        --timestamp
        coalesce(
            safe.parse_timestamp('%FT%H:%M:%E*S', tran_date),
            safe.parse_timestamp('%FT%H:%M:%E*S%Ez', tran_date)
        ) as tran_at,
        coalesce(
            safe.parse_timestamp('%FT%H:%M:%E*S', receive_date),
            safe.parse_timestamp('%FT%H:%M:%E*S%Ez', receive_date)
        ) as received_at,

        --other infor
        barcode,
        package,

        --metric
        is_approved,
        unit_price,
        amount,
        quantity,
        quantity_adj,
        quantity_extra,

        --system
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_warehouse_balance_actual_hashid

    from sources

)

select * from renamed
