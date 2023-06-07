with sources as (
    select * from {{ ref('acc_warehouse__unioned') }}
),

renamed as (
    select
        {{ dbt_utils.surrogate_key(['pr_key_warehouse','job_id']) }} as unique_id,

        -- primary key
        pr_key_warehouse,
        pr_key,
        pr_key_detail,
        pr_detail_id,

        --other id
        tran_id,
        job_id as outlet_code,
        upper(item_id) as item_id,

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
        product_id,
        supply_id,
        organization_id,
        expense_id,

        tran_no,
        lot_no,

        --timestamp
        coalesce(
            safe.parse_timestamp('%FT%H:%M:%E*S', tran_date),
            safe.parse_timestamp('%FT%H:%M:%E*S%Ez', tran_date)
        ) as tran_at,
        coalesce(
            safe.parse_timestamp('%FT%H:%M:%E*S', use_date),
            safe.parse_timestamp('%FT%H:%M:%E*S%Ez', use_date)
        ) as use_at,
        coalesce(
            safe.parse_timestamp('%FT%H:%M:%E*S', receive_date),
            safe.parse_timestamp('%FT%H:%M:%E*S%Ez', receive_date)
        ) as receive_at,
        coalesce(
            safe.parse_timestamp('%FT%H:%M:%E*S', last_modify_date),
            safe.parse_timestamp('%FT%H:%M:%E*S%Ez', last_modify_date)
        ) as last_modify_at,

        description,

        --metric
        quantity,
        quantity_extra,
        job_qty,
        unit_price,
        amount,

        --other information
        warning,
        comments,
        vat_tran_no,
        issue_receive,
        warehouse_id_issue,
        unit_id_actual,
        account_id,
        account_id_contra,
        extra_id_1,
        extra_id_2,
        machine_id

        --system
        --_airbyte_ab_id,
        --_airbyte_emitted_at,
        --_airbyte_normalized_at,
        --_airbyte_warehouse_analytics_hashid

    from sources
    qualify row_number() over (partition by unique_id order by _airbyte_emitted_at desc) = 1

)

select * from renamed
