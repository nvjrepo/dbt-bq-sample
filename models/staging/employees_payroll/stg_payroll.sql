with sources as (
    select * from {{ source('human_resources','payroll') }}
),

renamed as (
    select
        {{ dbt_utils.surrogate_key(['employee_code_and_month']) }} as unique_id,
        --employee_code_and_month,

        --employee infor
        employee_code_new,
        employee_code_old,
        employee_full_name,
        contract_type as contract_type_original,
        contract_type_2 as contract_type,
        case when gender = 'Nam' then 'male' else 'female' end as gender,
        age,
        position,
        `level` as position_level,

        --company info
        entity,
        companies,
        bu,
        outlet_code,

        --time
        `start_date` as start_at,
        months as payroll_month,
        left_date,
        day_of_birth,

        --day
        working_day_theory,
        working_day_actual,
        total_working_day,
        remaining_working_hour,

        --salary
        direct_salary,
        basic_salary,
        insurance_salary,
        holiday_salary,
        rostered_day_off_salary,
        wedding_funeral_off_salary,
        daily_salary,
        holiday_salary_300_p,
        salary_kpi_inclusive,
        other_salary,
        total_salary,

        --OT
        overtime_100_p,
        overtime_150_p,
        overtime_200_p,
        overtime_300_p,
        overtime_100_p_salary,
        overtime_150_p_salary,
        overtime_200_p_salary,
        overtime,

        --hour
        salary_working_hour,
        salary_per_hour,
        actual_salary_hour as paid_off_leave_salary, --edit to leave_day_salary
        total_salary_by_working_hour,
        actual_salary_day,
        total_over_time_hour,
        total_working_hour,

        --bonus
        probation_salary,
        probation_kpi_salary,
        kpi_bonus,
        month_13_salary,
        kpi_performance_p,
        sale_bonus,
        year_end_bonus,

        --deduction
        uniform_deductions,
        other_deductions,
        dependants,
        total_deduction,

        --insruance
        si_175,
        si_8,
        hi_3,
        hi_1_5,
        ui_1,
        si_hi_ui_105,
        si_hi_ui_215,

        si_company_wide,
        si_employer,
        si_employee,

        social_insurance_status,

        --arrears
        arrears_employer,
        arrears_employee,

        --union
        union_fee,
        union_2,
        union_after_Tax,
        union_expense,
        total_union_expenses,

        --tax
        income_tax,
        net_income_after_tax,
        cit,
        cit_deduction,

        --income
        total_income,
        taxable_income,
        net_take_home,
        revenue,

        note,

        --col
        total_amount,
        col,
        col_exclude_bonus,

        --internal group
        acc_1,
        acc_2,
        acc_3,
        hr_1,
        hr_2,
        average_salary_by_group,
        restaurant_count,

        --experience
        experience,
        experience_year,
        experience_month,
        experience_category,


        --status
        is_quit as employee_status,
        is_new_employee,
        is_probation_contract,
        lower(is_quit) = 'on' as is_active,
        lower(is_quit) = 'leave' as is_leave,

        --leave infor
        leave_days,
        leave_expense_remaining,
        leave_day,
        holiday,
        coalesce(leave_day_cleared,0) as leave_day_cleared,
        leave_expense_cleared,
        rostered_day_off,
        wedding_funeral_off,
        rostered_day_off_ot,
        rostered_day_off_ot_salary,

        --allowance
        others_allowance,
        allowance,
        meal_allowance_actual,
        allowance_working_hour,
        meal_allowance,
        phone_allowance,
        travel_allowance,
        other_allowance,
        clothe_allowance,
        business_trip_allowance,
        house_allowance,
        children_allowance,
        project_allowance,

        phone_allowance_actual,
        travel_allowance_actual,
        other_allowance_actual,
        total_allowance_actual,
        salary_allowance_inclusive,

        labour_points


        --empty
        --empty10,
        --empty9,
        --empty8,
        --empty7,
        --empty6,
        --empty5,
        --empty4,
        --empty3,
        --empty2,
        --empty1

    from sources
)

select * from renamed
