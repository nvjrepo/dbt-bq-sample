with payroll as (
    select * from {{ ref('stg_payroll') }}
),

bis_code as (
    select * from {{ ref('stg_gg_sheet__bis_code') }}
),

payroll_records as (
    select * from {{ ref('stg_base_vn_payroll__records') }}
),

employees as (
    select
        employee_code,
        is_terminated,
        terminated_at,
        date_trunc(terminated_at, month) as terminated_month
    from {{ ref('stg_base_vn_hrm__employees') }}
    where is_terminated
),


unioned as (
    select
        unique_id,
        cast(payroll_month as date) as payroll_month,
        employee_code_new as employee_code,
        employee_status,
        is_active,
        case
            when contract_type_original = 'Parttime' then contract_type_original
            when contract_type_original = 'Freelance' then contract_type_original
            else 'Fulltime'
        end as contract_type,
        position_level,

        outlet_code,

        leave_days,
        leave_expense_remaining,
        leave_day_cleared,
        leave_expense_cleared,

        col,
        gender,
        age,

        is_leave,
        start_at,

        total_salary,
        basic_salary,
        total_working_hour

    from payroll

    union all

    select
        record_id as unique_id,
        payroll_month,
        employee_code,
        employee_status,
        is_active,
        contract_type,
        position_level,

        outlet_code,

        leave_days,
        0 as leave_expense_remaining,
        0 as leave_day_cleared,
        0 as leave_expense_cleared,

        col,
        gender,
        age,

        is_leave,
        start_at,

        tong_luong,
        basic_salary,
        total_working_hour

    from payroll_records
),

joined_logics as (
    select
        unioned.*,
        date_trunc(unioned.start_at, month) = date(unioned.payroll_month) as is_new_hire,

        case
            when lower(unioned.employee_status) = 'on' then
                case
                    when date_trunc(unioned.start_at, month) = date(unioned.payroll_month) then 'new_hire'
                    else 'active employees'
                end
            else 'others'
        end as employee_status_grouping,

        case
            when unioned.age < 18 then '<18'
            when unioned.age >= 18 and unioned.age < 22 then '18-22'
            when unioned.age >= 22 and unioned.age <= 27 then '22-27'
            when unioned.age >= 28 and unioned.age <= 32 then '28-32'
            when unioned.age > 32 then '32+'
        end as age_group,

        if(unioned.total_salary = 0, unioned.basic_salary, unioned.total_salary) as salary,

        employees.* except (employee_code),

        --statistic
        1 as total_employee_count,
        if(employees.is_terminated, 1, 0) as left_employee_count,
        if(unioned.gender = 'female', 1, 0) as female_employee_count,
        if(unioned.gender != 'female', 1, 0) as male_employee_count,
        if(unioned.contract_type = 'Fulltime', 1, 0) as fulltime_employee_count,
        if(unioned.contract_type = 'Parttime', 1, 0) as partime_employee_count,
        if(unioned.contract_type = 'Freelance', 1, 0) as freelance_employee_count

    from unioned
    left join employees
        on unioned.employee_code = employees.employee_code
            and unioned.payroll_month = employees.terminated_month
),

final as (
    select
        joined_logics.*,
        bis_code.bu_1 as bu,
        bis_code.department_code,
        bis_code.department_name,
        bis_code.accessed_email

    from joined_logics
    left join bis_code
              on joined_logics.outlet_code = bis_code.outlet_code
-- where bis_code.outlet_code is not null
)

select * from final
