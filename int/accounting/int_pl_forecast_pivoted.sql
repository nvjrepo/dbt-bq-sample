with target_unpivoted as (
{{ dbt_utils.unpivot(ref('stg_acc_gcs__pl_forecast'), 
       cast_to='float64',
       field_name='date_month',
       value_name='forecasted_amount', 
       exclude=['expense_id','pl_section','unique_id','forecast_year']) 
    }}
),

cashflow_budget as (
    select
        expense_id,
        pl_section,
        date(
            concat(
                forecast_year,
                '-',
                replace(date_month, 'm_', ''),
                '-01'
            )
        ) as date_month,
        forecasted_amount

    from target_unpivoted
)

select * from cashflow_budget
