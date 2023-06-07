{%- set schemas_to_convert1=[
    'employee_id',
    'tax_no',
    'inso_no',
    'ssn_no'
]-%}

{%- set schemas_to_convert2=[
    'inso_place',
    'ssn_place'
]-%}


with sources as (
    select * from {{ source('base_hrm', 'employee_legals') }}
),

renamed as (
    select
        --primary keys
        id as employee_legal_id,

        --id
        {% for schema_name in schemas_to_convert1 -%}
            {{ convert_string_to_null(schema_name) }},
        {% endfor %}

        --time
        case
            when ssn_date='00/01/1900' then null
            else datetime(parse_date('%d/%m/%Y',ssn_date))
        end as ssn_at,
        --datetime(parse_date('%d/%m/%Y',date)) as legal_at,
        loaded_date as loaded_at,

        --insurance & identifical information
        {% for schema_name in schemas_to_convert1 -%}
            {{ convert_string_to_null(schema_name,adds='employee_legal_') }},
        {% endfor %}
        
        --boolean
        cast(personal_deduction as int64) =1 as has_personal_deduction
        


    from sources

)

select * from renamed