{%- set schemas_to_convert=[
    'starred',
    'deadline',
    'office_name'
]-%}

with sources as (
    select * from {{ source('base_ehiring', 'opening') }}
),

renamed as (
    select
        id	as opening_id,
        talent_pool_id,
        dept_id,

        --time
        datetime(timestamp_seconds(cast(stime as int64)),'+07') as stime,
        datetime(timestamp_seconds(cast(etime as int64)),'+07') as etime,
        datetime(timestamp_seconds(cast(since as int64)),'+07') as since,
        datetime(timestamp_seconds(cast(last_update as int64)),'+07') as last_update,
        loaded_date,

        --opening information
        codename as opening_code,
        name as opening_name,
        period as opening_period,
        status as opening_status,

        --numertic
        cast(replace(regexp_substr(salary, '[0-9,]+',1,2),',','') as int64) as salary_min,
        cast(replace(regexp_substr(salary, '[0-9,]+',1,1),',','') as int64) as salary_max,
        cast(num_positions as int64) as intake_numbers,
        
        --other information
        {% for schema_name in schemas_to_convert -%}
            {{ convert_string_to_null(schema_name) }},
        {% endfor %}
        metatype,
        company_name,
        offices,
        locations

    from sources

)

select * from renamed