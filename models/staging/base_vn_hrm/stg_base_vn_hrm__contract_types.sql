{%- set schemas_to_convert1=[
    'creator_id',
    'tax_id',
    'insurance_id'
] -%}

{%- set schemas_to_convert2=[
    'name',
    'content',
    'metatype'
] -%}

with sources as (
    select * from {{ source('base_hrm', 'contract_types') }}
),

renamed as (
    select
        --id
        id as contract_type_id,
        {% for schema_name in schemas_to_convert1 -%}
            cast({{ schema_name }} as string) as {{ schema_name }},
        {% endfor %}

        --time
        datetime(timestamp_seconds(cast(since as int64)), '+07') as since_at,
        loaded_date as loaded_at,

        --contract information
        {% for schema_name in schemas_to_convert2 -%}
            {{ convert_string_to_null(schema_name,adds='contract_type_') }},
        {% endfor %}

        --boolean
        cast(is_probation as int64) = 1 as is_probation,
        cast(config_probation as int64) = 1 as is_config_probation,

        --other infor
        json_value_array(followers) as followers,
        json_value_array(form) as form,
        json_value_array(files) as files

    from sources

)

select * from renamed
