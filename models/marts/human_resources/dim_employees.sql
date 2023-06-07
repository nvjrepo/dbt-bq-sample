{%- set genders=['male','female'] -%}

with employees as (
    select * from {{ ref('stg_base_vn_hrm__employees') }}
),

employee_types as (
    select * from {{ ref('stg_base_vn_hrm__employee_types') }}
),

areas as (
    select * from {{ ref('stg_base_vn_hrm__areas') }}
),

position_types as (
    select * from {{ ref('stg_base_vn_hrm__position_types') }}
),

positions as (
    select * from {{ ref('stg_base_vn_hrm__positions') }}
),

payroll_records as (
    select * from {{ ref('stg_base_vn_payroll__records') }}
),

bis_code as (
    select * from {{ ref('stg_gg_sheet__bis_code') }}
),

joined as (
    select
        employees.employee_id,
        employees.employee_code,
        employees.started_at,
        employees.terminated_at,
        employees.employee_name,
        employees.employee_title,
        employees.employee_email,
        employees.employee_phone,
        employees.employee_gender,
        case
            when employees.salary != 0 then employees.salary
            when employees.salary = 0 then employees.basic_salary
            else 0
        end as salary,
        employees.is_terminated,
        employee_types.employee_type_name as contract_type,
        areas.outlet_code,
        positions.position_name,
        position_types.position_type_name as position_type,
        bis_code.bu_1 as bu,
        bis_code.department_code,
        bis_code.department_name,
        bis_code.accessed_email,
        coalesce(pr1.total_salaries,pr2.total_salaries) as col,
        coalesce(pr1.total_working_day,pr2.total_working_day) as working_hour,
        extract(year from current_date()) - employees.dob_year as employee_age

    from employees
    left join employee_types
        on employees.employee_type_id = employee_types.employee_type_id
    left join areas
        on employees.area_id = areas.area_id
    left join positions
        on employees.position_id = positions.position_id
    left join position_types
        on positions.position_type_id = position_types.position_type_id
    left join bis_code
        on areas.outlet_code = bis_code.outlet_code
    left join payroll_records as pr1
        on employees.employee_id = pr1.employee_id
            and date(pr1.payroll_month) = date_sub(date_trunc(current_date(),month), interval 1 month)
    left join payroll_records as pr2
        on employees.employee_id = pr2.employee_id
            and date(pr2.payroll_month) = date_sub(date_trunc(current_date(),month), interval 2 month)

),

logics as (
    select
        *,
        {{ object_segment('employee_') }},

        date_trunc(started_at,month) = date_trunc(current_date(),month) as is_new_hire,

        contract_type != 'Parttime'
        and not is_terminated
        as is_active_fulltime,

        contract_type = 'Parttime'
        and not is_terminated
        as is_active_parttime,

        contract_type != 'Parttime'
        and is_terminated
        as is_turnover_fulltime,

        contract_type = 'Parttime'
        and is_terminated
        as is_turnover_parttime,

        {% for gender in genders -%}employee_gender = '{{ gender }}'
        and not is_terminated
            as is_active_{{ gender }},

            employee_gender = '{{ gender }}'
            and contract_type != 'Parttime'
            and not is_terminated
            as is_fulltime_{{ gender }},

            employee_gender = '{{ gender }}'
            and contract_type = 'Parttime'
            and not is_terminated
            as is_parttime_{{ gender }},

            employee_gender = '{{ gender }}'
            and is_terminated
            as is_turnover_{{ gender }}{{ ',' if not loop.last }}

        {% endfor -%}

    
    from joined
)

select * from logics
