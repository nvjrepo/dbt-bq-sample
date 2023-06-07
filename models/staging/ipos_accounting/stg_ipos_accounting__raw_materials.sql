with sources as  (
    select * from {{ source('accounting_all','DM_ITEM') }}
),

renamed as (
    select
        --id
        item_id,
        user_id,
        unit_id,
        unit_id_extra,

        --item infor
        item_class_id,
        item_class1_id,
        item_type_id,
        item_name,
        
        --addt infor
        cost_method,
        package,
        barcode,

        -- account_id
        account_id_cost,
        account_id_income,
        account_id_return,
        account_id_discount,
        account_id_discount2,
        account_id_sale_cost,
        vat_tax_id,


        -- boolean 
        is_warehouse_balance = 1 as is_warehouse_balance,
        is_parent = 1 as is_parent,
        active = 1 as is_active,
        bom_auto = 1 as is_bom_auto,

        -- metrics
        import_tax_rate,
        export_tax_rate,
        bom_by_amount,
        luxury_tax_rate,

        --system		
        _airbyte_ab_id,		
        _airbyte_emitted_at,	
        _airbyte_normalized_at,	
        _airbyte_DM_ITEM_hashid

    from sources

)

select * from renamed


