**{% docs fct_weekly_col_index %}
The model is used to finalize time log and calculate col for employee
    1. Dedup data
    From data loaded, there are duplicated records accounting for 1 employee of the same check in date.First, deduping the records using the latest loaded date:
    `rank () over (partition by employee_id, date order by loaded_at desc)`
    Finding first time checkin and last time checkout
    `first_value(start_at) over 
        (partition by employee_id, date order by start_at rows between unbounded preceding and unbounded following) as check_in_at,
    last_value(end_dat) over 
        (partition by employee_id, date order by end_at rows between unbounded preceding and unbounded following) as check_out_at`
    Then we must using the lasted time records only
    row_number () over (partition by employee_id, date order by last_update_at desc) =1
    2. Mapping with employee dim data to find salary and contractime 
{% enddocs %}**

**{% docs fct_employee_payrolls %}
The model contained monthly COL employees and dim of that employees including 
    - Age 
    - Gender
    - Contract type (*)
    - Is terminated yet
    - Terminated date (**)
    - Salary
Some notes:
 (*) Contract types (origin from column - phan_loai_nhan_su of stg_base_vn_payroll_records) if Partime/ Freelance -> defined as it's. if not -> classified as "Fultime".
 (**) Terminated date from stg_base_vn_hrm__employees mapping with payroll_month   
 
{% enddocs %}**