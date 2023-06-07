with checkin_logs as (
    select * from {{ ref('stg_base_vn_checkin__logs') }}
    where checkin_month >= '2023-01-01'
),

employees as (
    select * from {{ ref('dim_employees') }}
),

--full time convert to hour = Lương theo tháng / công chuẩn / 8
--công chuẩn = tổng số ngày trong tháng - số ngày chủ nhật trong tháng

check_in_out as (
    select
        unique_id,
        employee_id,
        checkin_date,
        checkin_at,
        lead(checkin_at) over (partition by checkin_log_id order by checkin_at) as checkout_at,

    from checkin_logs
    qualify checkout_at is not null
),

joined as (
    select
        check_in_out.*,
        employees.outlet_code,
        employees.employee_name,
        employees.salary,
        employees.contract_type,
        timestamp_diff(checkout_at, checkin_at, minute) as minute_logged

    from check_in_out
    left join employees
        on check_in_out.employee_id = employees.employee_id

),

parttime_col as (
    select
        *,
        minute_logged*salary/60 as labour_cost
    
    from joined
    where contract_type='Parttime'
),

fulltime_col as (
    select
        *,
        salary/extract(day from last_day(checkin_at)) as labour_cost
    
    from joined
    where contract_type!='Parttime'
    qualify row_number() over (partition by employee_id,date(checkin_at) order by checkin_at)=1
),

final as (
    select * from parttime_col
    union all
    select * from fulltime_col
)

select * from final

			
