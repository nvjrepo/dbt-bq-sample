{%- set schemas_to_convert=[
    'content',
    'color',
    'metatype'
]-%}

with sources as (
    select * from {{ source('base_hrm', 'team') }}
),

renamed as (
    select 
        --id
        id as team_id,
        area_id,
        {{ convert_string_to_null('dept_id') }},
        creator_id,

        --time
        datetime(timestamp_seconds(cast(since as int64)), '+07') as since_at,
        datetime(timestamp_seconds(cast(last_update as int64)), '+07') as last_update_at,
        loaded_date,

        --dimension
        code as team_code,
        name as team_name,

        --other
        json_value_array(owners) as owners,
        json_value_array(watchers) as watchers,
        json_value_array(followers) as followers,
        {% for schema_name in schemas_to_convert -%}
            {{ convert_string_to_null(schema_name) }}{{ ',' if not loop.last }}
        {% endfor %}

    from sources
)

select * from renamed
