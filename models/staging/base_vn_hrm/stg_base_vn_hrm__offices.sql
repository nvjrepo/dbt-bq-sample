{%- set schemas_to_convert1=[
    'name',
    'address',
    'content',
    'type',
    'phone',
    'email'
]-%}

{%- set schemas_to_convert2=[
    'data_code',
    'data_phone',
    'data_email',
    'hid',
    'token'
]-%}


with sources as (
    select * from {{ source('base_hrm', 'office') }}
),

renamed as (
    select 
        --key
        id as office_id,
        user_id,

        --time
        datetime(timestamp_seconds(cast(since as int64)), '+07') as since_at, 
        datetime(timestamp_seconds(cast(last_update as int64)), '+07') as last_update_at,
        loaded_date as loaded_at,

        --dimensions
        {% for schema_name in schemas_to_convert1 -%}
            {{ convert_string_to_null(schema_name,adds='office_') }},
        {% endfor %}

        --boolean
        cast(hq as int64) = 1 as is_headquarter,

        --number
        cast (num_people as float64) num_people,

        --others
        {% for schema_name in schemas_to_convert2 -%}
            {{ convert_string_to_null(schema_name) }},
        {% endfor %}
        metatype

    from sources
)

select * from renamed
