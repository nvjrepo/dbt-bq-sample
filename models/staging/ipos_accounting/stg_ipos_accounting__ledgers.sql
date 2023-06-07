with sources as (
    select * from {{ ref('ledgers__unioned') }}
),

renamed as (
    select
        {{ dbt_utils.surrogate_key(['_dbt_source_relation','pr_key_ledger']) }} as unique_id,
        --pr
        cast(pr_key_ledger as int) as pr_key_ledger,
        cast(pr_key as int) as pr_key,
        pr_detail_id,
        pr_detail_id_contra,
        cast(pr_key_detail as int) as pr_key_detail,

        --chart of account
        account_id,
        account_id_contra,

        --transaction
        tran_id,
        coalesce(
            safe.parse_timestamp('%FT%H:%M:%E*S', tran_date),
            safe.parse_timestamp('%FT%H:%M:%E*S%Ez', tran_date)
        ) as tran_at,

        tran_no,
        vat_tran_no,
        vat_tran_serie,
        coalesce(
            safe.parse_timestamp('%FT%H:%M:%E*S', vat_tran_date),
            safe.parse_timestamp('%FT%H:%M:%E*S%Ez', vat_tran_date)
        ) as vat_tran_at,

        debit_credit,

        --expense
        expense_id,
        expense_id_contra,

        --unit,pieces 
        currency_id,
        cast(exchange_rate as float64) as exchange_rate,
        cast(quantity as float64) as quantity,
        cast(unit_price as float64) as unit_price,
        cast(unit_price_orig as float64) as unit_price_orig,
        case when debit_credit = 'DEB' then cast(amount as float64) else cast(amount as float64) * -1 end as amount,
        case when debit_credit = 'DEB' then cast(amount_orig as float64) else cast(amount_orig as float64) * -1 end as amount_orig,

        --payment
        payment_method_id,
        payment_term_id,
        coalesce(
            safe.parse_timestamp('%FT%H:%M:%E*S', payment_date),
            safe.parse_timestamp('%FT%H:%M:%E*S%Ez', payment_date)
        ) as payment_at,

        --job
        job_id,
        job_id_contra,

        --extra
        extra_id_1,
        extra_id_2,

        --item
        item_id,
        item_id_contra,

        --bank
        bank_id,
        bank_id_contra,

        --product
        product_id,
        product_id_contra,

        --organization
        organization_id,
        organization_id_delivery,

        --other information
        contact_person,
        address,
        comments,
        description,
        employee_id,
        reference_no,

        --boolean
        case when cast(is_booked as int) = 1 then 'true' else 'false' end as is_booked,
        case when cast(is_product_cost as int) = 1 then 'true' else 'false' end as is_product_cost,

        --register
        register_no,
        coalesce(
            safe.parse_timestamp('%FT%H:%M:%E*S', register_date),
            safe.parse_timestamp('%FT%H:%M:%E*S%Ez', register_date)
        ) as register_at,
        coalesce(
            safe.parse_timestamp('%FT%H:%M:%E*S', last_modify_date),
            safe.parse_timestamp('%FT%H:%M:%E*S%Ez', last_modify_date)
        ) as last_modified_at,

        --logic
        case
            when job_id in (
                'HCM-CK1-BC',
                'HCM-CK2-NK',
                'HCM-SC1',
                'HCM-BD1',
                'HCM-MKT1',
                'HCM-ACC1',
                'HCM-HR1',
                'HCM-IC1',
                'HCM-HO1',
                'VP',
                'P-TM',
                'P-KT'
            ) then 'HCM-HO1'
            when job_id in (
                'HCM-CK-PACC',
                'HCM-CK1-BC',
                'HCM-WH1-PACC',
                'BEP'
            ) then 'HCM-WH1-PACC'
            else job_id
        end as branch,

        --system
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_ledgers_analytics_hashid

    from sources

),

deduped as (
    select * from renamed
    qualify row_number() over (partition by unique_id order by _airbyte_emitted_at desc) = 1
)

select * from deduped

        --pr_detail_class_id,
        --pr_detail_name,
        --cast(pr_key_ctu as int) as pr_key_ctu,
        --job_name,
        --job_class_id,
        --extra_name_1,
        --extra_name_2,
        --item_class1_id,
        --item_product_class1_id,
        --item_product_class_id,
        --item_class_id,
        --item_product_name,
        --item_name,
        --expense_name,
        --expense_class_id,
        --account_name,
        --account_name_contra,
        --account_name_uls,
        --account_name_contra_uls,
        --bank_name,
        --bank_branch,
        --product_name,
        --product_class_id,
