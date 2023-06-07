with cashflow_actual as (
    select
        date_trunc(tran_at,month) as tran_month,
        expense_id as cf_code,
        sum(amount) as actual

    from {{ ref('stg_ipos_accounting__ledgers') }}
    where true
        and date(tran_at) >= '2023-01-01'
        and (
                account_id like '111%' 
                    or account_id like '112%'
        )
    {{ dbt_utils.group_by(2) }}

),

cashflow_forecast as (
    select * from {{ ref('int_cashflow_forecast_pivoted') }}
),

final as (
    select
        {{ dbt_utils.surrogate_key(['cashflow_forecast.date_month','cashflow_forecast.cf_code']) }} as unique_id,
        cashflow_forecast.date_month,
        cashflow_forecast.cf_code,
        cashflow_forecast.cf_name,
        cashflow_forecast.cf_segment,
        cashflow_forecast.cf_sector,
        cashflow_forecast.forecasted_amount as forecast,
        cashflow_actual.actual,

        cashflow_forecast.date_month <= date_trunc(current_date(),month)
            and extract(year from cashflow_forecast.date_month) =  extract(year from current_date())
        as is_ytd

    from cashflow_forecast
    left join cashflow_actual
        on cashflow_actual.cf_code = cashflow_forecast.cf_code
            and date(cashflow_actual.tran_month) = cashflow_forecast.date_month
)

select * from final