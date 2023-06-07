with sources as  (
    select * from {{ source('ipos_sale','dm_item') }}
),

renamed as (
    select
        --id
        item_id as sku_code,
        item_type_id,
        item_class_id,

        --datetime
        effective_date,
        expire_date,

        --item infor
        item_name,
        description,
        case
            when lower(item_category)='food' then 'Food'
            when lower(item_category)='beverage' then 'Beverage'
            else 'Other'
        end as item_category,
        item_group,
        
        --other information
        user_id,
        unit_id,     
        customizations,
        item_id_eat_with,
        item_id_mapping,
        currency_type_id,

        item_id_exclude,
        item_string,
        
        --numeric
        ta_price,
        ots_tax,
        quantity_per_day,
        cost_price,
        ots_price,
        point,
        quantity_limit,
        quantity_default,
        time_cooking,
        ta_tax,

        --boolean
        is_print_label=1 as is_print_label,
        is_gift=1 as is_gift,
        price_change=1 as is_price_change,
        workstation_id=1 as is_workstation_id,
        is_material=1 as is_material,
        list_order=1 as is_list_order,
        is_eat_with=1 as is_eat_with,
        is_parent=1 as is_parent,
        is_sub=1 as is_sub,
        is_allow_discount=1 as is_allow_discount,
        is_fc=1 as is_fc,
        is_foreign=1 as is_foreign,
        is_service=1 as is_service,
        allow_take_away=1 as is_allow_take_away,
        process_index=1 as is_process_index,
        show_on_web=1 as is_show_on_web,
        active=1 as is_active,
        is_kit=1 as is_kit,
        show_price_on_web=1 as is_show_price_on_web


    from sources
    qualify row_number() over (partition by item_id order by effective_date desc) = 1

)

select * from renamed

--item_image,
--item_color,