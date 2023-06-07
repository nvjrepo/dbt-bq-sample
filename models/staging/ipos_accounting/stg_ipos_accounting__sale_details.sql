with sources as  (
    select * from {{ ref('acc_sale_details__unioned') }}
),

renamed as (
    select
        {{ dbt_utils.surrogate_key(['_dbt_source_relation','pr_key']) }} as unique_id,
        --pr, fr key
        cast(pr_key as int) pr_key,
        cast(fr_key as int) fr_key,

        --- account 
        cast(list_order as int) list_order,
        coalesce(
            safe.parse_timestamp('%FT%H:%M:%E*S', tran_date),
            safe.parse_timestamp('%FT%H:%M:%E*S%Ez', tran_date)
        ) as tran_at,
        coalesce(
            safe.parse_timestamp('%FT%H:%M:%E*S', receive_date),
            safe.parse_timestamp('%FT%H:%M:%E*S%Ez', receive_date)
        ) as received_at,
        coalesce(
            safe.parse_timestamp('%FT%H:%M:%E*S', last_modify_date),
            safe.parse_timestamp('%FT%H:%M:%E*S%Ez', last_modify_date)
        ) as last_modified_at,

       
        --item & stock
        ---item info

        item_id,
        parent_item_id,
        description,
        barcode,

        ---warehouse & stock quantity
        warehouse_id,
        price_level_id,
        product_id,   
        unit_id,
        quantity,
        quantity_expected,
        quantity_extra,
        quantity_wh,
        quantity_wb,
        openning_quantity,
        closing_quantity,

        ---stock unit price
        cast(unit_price_orig as int) unit_price_orig,
        cast(unit_price as int) unit_price,
        cast(unit_price_wh as int) unit_price_wh,
        cast(unit_price_pre_vat as int) unit_price_pre_vat,
        cast(unit_price_pre_vat_orig as int) unit_price_pre_vat_orig,        
        

        --tax
        cast(vat_income_amount_orig as int) vat_income_amount_orig,
        cast(vat_income_amount as int) vat_income_amount,

        vat_tax_id,

        cast(vat_tax_rate as int) vat_tax_rate,
        cast(vat_tax_amount as int) vat_tax_amount,
        cast(vat_tax_amount_orig as int) vat_tax_amount_orig,

        cast(export_tax_rate as int) export_tax_rate,
        cast(export_tax_amount as int) export_tax_amount,
        cast(export_tax_amount_orig as int) export_tax_amount_orig,

        cast(lux_tax_rate as int) lux_tax_rate,
        cast(lux_tax_amount as int) lux_tax_amount,
        cast(lux_tax_amount_orig as int) lux_tax_amount_orig,
        
        --sale amount
        cast(amount_orig as int) amount_orig,
        cast(amount as int) amount,

        cast(discount_rate as int) discount_rate,
        cast(discount_amount as int) discount_amount,
        cast(discount_amount_orig as int) discount_amount_orig,
        cast(discount2_rate as int) discount2_rate,
        cast(discount2_amount as int) discount2_amount,
        cast(discount2_amount_orig as int) discount2_amount_orig,
        cast(real_discount_amount as int) real_discount_amount,
        cast(real_discount_amount_orig as int) real_discount_amount_orig,
        cast(real_discount2_amount as int) real_discount2_amount,
        cast(real_discount2_amount_orig as int) real_discount2_amount_orig,


        cast(income_amount as int) income_amount,
        cast(income_amount_orig as int) income_amount_orig,  

        --income +VAT: 
        cast(total_amount as int) total_amount,
        cast(total_amount_orig as int) total_amount_orig,
        --COGS

        cog_unit_price,
        cog_unit_price_orig,
        cog_amount,
        cog_amount_orig,
        
        cast(ship_charge as int) ship_charge,
        cast(ship_charge_orig as int) ship_charge_orig,

        ---Sale deduct cost
        sale_cost_rate,
        sale_cost,
        sale_cost_orig,   
        real_sale_cost,
        real_sale_cost_orig,
        
   
        ---account booking     
        account_id,
        account_id_cost,
        account_id_income,
        pr_detail_id_item,
        expense_id,
        account_id_discount,
        account_id_discount2,        
        
        ---others
        
        warning,
        machine_id,
        organization_id,
        employee_id,


        --booking
        job_id,
        job_qty,
        if (is_up=1, true, false) as is_up ,--boolean
        
        --system
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_sale_detail_analytics_hashid       

    from sources

),

deduped as (
    select * from renamed
    qualify row_number() over (partition by unique_id order by _airbyte_emitted_at desc) = 1
)

select * from deduped


