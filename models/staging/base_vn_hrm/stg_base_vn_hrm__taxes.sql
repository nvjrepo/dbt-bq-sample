{%- set schemas_to_convert=[
    'content',
    'config_percent',
    'color',
    'eba'
]-%}

with sources as (
    select * from {{ source('base_hrm', 'tax') }}
),

renamed as (
    select 
        --id
        id as tax_id,
        {{ convert_string_to_null('user_id') }},

        --time
        datetime(timestamp_seconds(cast(since as int64)), '+07') as since_at,
        datetime(timestamp_seconds(cast(last_update as int64)), '+07') as last_update_at,
        loaded_date,

        --dimension
        code as tax_code,
        name as tax_name,
        type as tax_type,
        status as tax_status,
        cast(percent as float64) as tax_percent,

        --other
        username,
        metatype,
        {% for schema_name in schemas_to_convert -%}
            {{ convert_string_to_null(schema_name) }}{{ ',' if not loop.last }}
        {% endfor %}

    from sources
)

select * from renamed
