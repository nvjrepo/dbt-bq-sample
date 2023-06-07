with sources as (
    select * from {{ source('ipos_sale','order_menu_log') }}
),

renamed as (
    select distinct
        unique_key as unique_id,

        --key id transaction
        safe_cast(pr_key as string) as order_id,
        safe_cast(pr_key_sale_detail as string) as pr_key_sale_detail,

        --others id
        safe_cast(item_id as string) as item_id,
        safe_cast(workstation_id as string) as workstation_id,
        initcap(safe_cast(unit_id as string)) as unit_id,
        safe_cast(reason_id as string) as reason_id,

        --dim
        case
            when lower(cast(user_name as string)) in('nan', '', 'none', '0') then string(null)
            else initcap(cast(user_name as string))
        end as order_user_name,
        case
            when lower(cast(description as string)) in('nan', '', 'none', '0') then string(null)
            else initcap(cast(description as string))
        end as order_description,
        {{ convert_string_to_null (schema_name = 'position', adds = 'order_') }},
        {{ convert_string_to_null (schema_name = 'order_index', adds = '') }},
        {{ convert_string_to_null (schema_name = 'print_name_menu', adds = '') }},
        {{ convert_int64_to_null (schema_name = 'order_hour' , adds = 'time_') }},
        {{ convert_int64_to_null (schema_name = 'order_minute' , adds = 'time_') }},
        {{ convert_int64_to_null (schema_name = 'service_hour' , adds = 'time_') }},
        {{ convert_int64_to_null (schema_name = 'service_minute' , adds = 'time_') }},
        {{ convert_int64_to_null (schema_name = 'time_cooking' , adds = '') }},
        {{ convert_int64_to_null (schema_name = 'time_pending' , adds = '') }},

        --metric
        quantity,

        --boolean
        remove = 1 as is_remove,
        is_done = 1 as is_is_done,

        --time    
        order_date,
        service_date,

        --others
        {{ convert_string_to_null (schema_name = 'sale_sign', adds = 'order_') }},

        --metadata
        loaded_date as loaded_at,
        data_source

    from sources

)

select * from renamed
