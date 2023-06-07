{%- set schemas_to_convert1=[
    'name',
    'type',
    'content',
    'status',
    'color',
    'eba'

]-%}

with sources as (
    select *
    from {{ source('base_hrm', 'insurance') }}
),

renamed as (
    select 
        --id
        id as insurance_id,
        user_id,

        --time
        datetime(timestamp_seconds(cast(since as int64)), '+07') as since_at, 
        datetime(timestamp_seconds(cast(last_update as int64)), '+07') as last_update_at,
        loaded_date as loaded_at,

        --dimensions
        {% for schema_name in schemas_to_convert -%}
            {{ convert_string_to_null(schema_name,adds='insurance_') }},
        {% endfor %}

        --number info
        cast(percent_company as float64) percent_company,
        cast(percent_employee as float64) percent_employee,

        --others
        username as user_name,
        code,
        metatype

    from sources
)
select *
from renamed
