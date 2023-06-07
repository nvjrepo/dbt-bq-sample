{%- set schemas_to_convert=[
    'name',
    'type',
    'content',
    'metatype'
]-%}

with sources as (
    select * from {{ source('base_hrm', 'area') }}
),

renamed as (
    select
        --id
        id as area_id,
        code as outlet_code,

        --time
        datetime(timestamp_seconds(cast(since as int64)),'+07')  as since_at,
        datetime(timestamp_seconds(cast(last_update as int64)),'+07')  as last_updated_at,
        loaded_date as loaded_at,
        
        --area information
        {% for schema_name in schemas_to_convert -%}
            {{ convert_string_to_null(schema_name,adds='area_') }}{{ ',' if not loop.last }}
        {% endfor %}

    from sources

)

select * from renamed