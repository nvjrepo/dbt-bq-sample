with target_unpivoted as  (
{{ dbt_utils.unpivot(ref('stg_acc_gcs__cashflow_forecast'), 
       cast_to='float64',
       field_name='date_month',
       value_name='forecasted_amount', 
       exclude=['section_order','cf_code','cf_name','forecast_year','unique_id']) 
    }}
),

cashflow_budget as (
    select
        cf_code,
        cf_name,
        date(
            concat(
                forecast_year,
                '-',
                replace(date_month,'m_',''),
                '-01'
            ) 
        ) as date_month,
        case 
            when safe_cast(regexp_substr(cf_code,'[0-9]+') as int64) <= 7 then 'Sale'
            else 'Expense'
        end as cf_segment,
        case 
            when safe_cast(regexp_substr(cf_code,'[0-9]+') as int64) <= 7 then 'Sale'
            when safe_cast(regexp_substr(cf_code,'[0-9]+') as int64) between 9 and 19 then 'Employee expense'
            when safe_cast(regexp_substr(cf_code,'[0-9]+') as int64) between 58 and 63 then 'Investing expense'
            when safe_cast(regexp_substr(cf_code,'[0-9]+') as int64) between 67 and 71 then 'Finance expense'
            else 'Operating expense'
        end as cf_sector,
                
        forecasted_amount
    
    from target_unpivoted
)

select * from cashflow_budget
