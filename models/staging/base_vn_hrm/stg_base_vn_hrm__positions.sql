{%- set schemas_to_convert2=[
    'promotion_reqs',
    'content',
    'objs',
    'image',
    'icon'
]-%}

with sources as (
    select * from {{ source('base_hrm', 'position') }}
),

renamed as (
    select 
        --id
        id as position_id,
        area_id,
        type_id as position_type_id,

        --time
        datetime(timestamp_seconds(cast(last_update as int64)), '+07') as last_update_at,
        loaded_date,

        --dimension
        code as position_code,
        name as position_name,
        status as position_status,

        --salary
        salary_min,
        salary_max,
        salary_strict,
        salary_hide,

        --other
        data_employee_hide,
        data___io,
        json_value_array(links) as links,
        {% for schema_name in schemas_to_convert1 -%}
            {{ convert_string_to_null(schema_name) }}{{ ',' if not loop.last }}
        {% endfor %}

    from sources
)

select * from renamed