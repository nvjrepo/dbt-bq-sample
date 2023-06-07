{%- set schemas_to_convert=[
    'code',
    'name',
    'content',
    'fte',
    'config_fte',
    'metatype'
]-%}

with sources as (
    select *
    from {{ source('base_hrm', 'employee_types') }}
),

renamed as (
    select
        -- primary keys
        id as employee_type_id,
        {{ convert_string_to_null('user_id') }},

        -- time
        datetime (timestamp_seconds ( cast (since as int64)), '+07') as since_at,
        datetime (timestamp_seconds ( cast (last_update as int64)), '+07') as last_update_at,
        loaded_date as loaded_at, 

        --employee types information
        {% for schema_name in schemas_to_convert -%}
            {{ convert_string_to_null(schema_name,adds='employee_type_') }}{{ ',' if not loop.last }}
        {% endfor %}

    from sources
)

select *
from renamed