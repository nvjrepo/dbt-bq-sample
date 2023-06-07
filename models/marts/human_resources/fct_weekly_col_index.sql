with shift_data as (
    select *
    from {{ ref('stg_base_vn_schedule__shift') }}
    where true
        and date >= '2023-01-01'
    qualify rank() over (partition by employee_id, date order by loaded_at desc) = 1
--since when finalized the checkout time, HR may enter on records for 1 employee on the same checkin date
),

checkin_logs as (
    select
        {{ dbt_utils.surrogate_key(['employee_id','date']) }} as unique_id,
        employee_id,
        date as checking_date,
        first_value(start_at) over
        (partition by employee_id, date order by start_at rows between unbounded preceding and unbounded following) as check_in_at,
        last_value(end_at) over
        (partition by employee_id, date order by end_at rows between unbounded preceding and unbounded following) as check_out_at

    from shift_data
    qualify row_number() over (partition by employee_id, date order by last_update_at desc) = 1
),

employees as (
    select * from {{ ref('dim_employees') }}
),

--full time convert to hour = Lương theo tháng / công chuẩn / 8
--công chuẩn = tổng số ngày trong tháng - số ngày chủ nhật trong tháng

joined as (
    select
        checkin_logs.*,
        employees.outlet_code,
        employees.employee_name,
        employees.salary,
        employees.contract_type,
        employees.position_type = 'Crew Member' as is_crew_labour,
        timestamp_diff(checkin_logs.check_out_at, checkin_logs.check_in_at, minute) as minute_logged,
        case
            when employees.position_type = 'Crew Member' then '024'
            else '025'
        end as expense_id
    from checkin_logs
    left join employees
        on checkin_logs.employee_id = employees.employee_id

),

parttime_col as (
    select
        *,
        minute_logged * salary / 60 as labour_cost

    from joined
    where contract_type = 'Parttime'
),

fulltime_col as (
    select
        *,
        salary / extract(day from last_day(check_in_at)) as labour_cost

    from joined
    where contract_type != 'Parttime'
)

select * from parttime_col
union all
select * from fulltime_col
