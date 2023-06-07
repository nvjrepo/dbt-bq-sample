{%- set schemas_to_convert=[
    'content',
    'objs',
    'promotion_reqs',
    'icon',
    'fill'
]-%}

with sources as (
    select * from {{ source('base_hrm', 'position_types') }}
),

renamed as (
    select 
        --id
        id as position_type_id,
        {{ convert_string_to_null('user_id') }},

        --time
        datetime(timestamp_seconds(cast(since as int64)), '+07') as since_at,
        datetime(timestamp_seconds(cast(last_update as int64)), '+07') as last_update_at,
        loaded_date,

        --dimension
        name as position_type_name,
        type as position_type,

        --other
        color,
        {% for schema_name in schemas_to_convert -%}
            {{ convert_string_to_null(schema_name) }}{{ ',' if not loop.last }}
        {% endfor %}

    from sources
)

select * from renamed