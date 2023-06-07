{%- set schemas_to_convert1=[
    'user_id',
    'tax_id',
    'insurance_id',
    'team_id',
    'timesheet_id',
    'office_id',
    'employee_type_id',
    'area_id',
    'position_id',
    'payroll_policy_id'
]-%}

{%- set schemas_to_convert2=[
    'profile_note',
    'profile_m1',
    'profile_m2',
    'profile_m3',
    'profile_p1',
    'profile_p2',
    'profile_p3',
    'profile_p4',
    'profile_p5',
    'bank_number',
    'bank_name',
    'bank_branch',
    'bank_holder'

]-%}

with sources as (
    select * from {{ source('base_hrm', 'employee') }}
),

renamed as (
    select
        --primary keys
        id as employee_id,

        --id
        code as employee_code,
        {% for schema_name in schemas_to_convert1 -%}
            {{ convert_string_to_null(schema_name) }},
        {% endfor %}

        --time
        datetime(timestamp_seconds(cast(start_date as int64)),'+07')  as started_at,
        case 
            when official_start_date='1970-01-01T07:00:00' then null 
            else datetime(timestamp_seconds(cast(official_start_date as int64)),'+07')  
        end as official_start_date,
        case 
            when terminated_date='1970-01-01T07:00:00' then null 
            else datetime(timestamp_seconds(cast(terminated_date as int64)),'+07')  
        end as terminated_at,
        datetime(timestamp_seconds(cast(last_update as int64)),'+07')  as last_updated_at,
        loaded_date as loaded_at,

        --pii
        first_name as employee_first_name,
        last_name as employee_last_name,
        name employee_name,
        title as employee_title,
        email as employee_email,
        case when cast(gender as int64)=1 then 'male' else 'female' end as employee_gender,
        case when left(phone,1)!='0' then concat('0',phone) else phone end as employee_phone,
        type as employee_type,
        cast(dob_day as int64) as dob_day,
        cast(dob_month as int64) as dob_month,
        cast(dob_year as int64) as dob_year,
        concat(dob_day,dob_month,dob_year) as dob,

        --profile
        profile_address,
        profile_ssn,
        profile_marital,
        profile_pob,
        profile_nationality,
        profile_permanent_residence,
        {% for schema_name in schemas_to_convert2 -%}
            {{ convert_string_to_null(schema_name) }},
        {% endfor %}

        --boolean
        cast(is_terminated as int64) = 1 as is_terminated,
        cast(is_primary as int64) =1 as is_primary,

        --salary
        cast(basic_salary as int64) as basic_salary,
        cast(salary as int64) as salary,

        --other infor
        metatype,
        form

    from sources

)

select * from renamed
