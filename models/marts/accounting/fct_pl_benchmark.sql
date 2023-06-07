with grouped as (
    select
        tran_month,
        branch,
        pl1,
        pl2,
        pl3,
        sum(amount) as amount

    from {{ ref('int_revenue_expenses') }}
    where is_monthly
    {{ dbt_utils.group_by(5) }}
),

revenue_expenses as (
    select
        *,
        safe_divide(amount,sum(coalesce(amount,0)) over (partition by tran_month,pl3)) as expense_index

    from grouped
    where true
        and date(tran_month) <= date_sub(current_date(), interval 1 month)
        --and date(tran_month) >= '2023-01-01'
        and pl1 not in ('Total Sales', 'Financial Income', 'Financial Income', '')
),

bis_code as (
    select * from {{ ref('stg_gg_sheet__bis_code') }}
),

pl_forecast as (
    select * from {{ ref('int_pl_forecast_pivoted') }}
),

pl_forecast_outlet as (
    select
        pl_forecast.expense_id,
        pl_forecast.date_month,
        pl_forecast.pl_section,
        revenue_expenses.branch,
        pl_forecast.forecasted_amount * revenue_expenses.expense_index as forecasted_amount

    from pl_forecast
    left join revenue_expenses
        on pl_forecast.date_month = date(revenue_expenses.tran_month)
            and pl_forecast.pl_section = revenue_expenses.pl3
),

final as (
    select
        {{ dbt_utils.surrogate_key(['revenue_expenses.tran_month',
                                    'revenue_expenses.branch',
                                    'revenue_expenses.pl1',
                                    'revenue_expenses.pl2',
                                    'revenue_expenses.pl3'
                                ]) }} as unique_id,
        revenue_expenses.tran_month,
        revenue_expenses.branch,
        revenue_expenses.pl1,
        revenue_expenses.pl2,
        revenue_expenses.pl3,
        -revenue_expenses.amount as actual,
        pl_forecast_outlet.forecasted_amount as forecast,
        -revenue_expenses.amount + 4000000 as budget,

        bis_code.bu_1 as bu,
        bis_code.department_name,
        bis_code.`zone` as region,
        bis_code.district,
        bis_code.accessed_email,

        date(revenue_expenses.tran_month) <= date_trunc(current_date(),month)
        and extract(year from revenue_expenses.tran_month) = extract(year from current_date())
        as is_ytd

    from revenue_expenses
    left join bis_code
        on revenue_expenses.branch = bis_code.outlet_code
    left join pl_forecast_outlet
        on revenue_expenses.branch = pl_forecast_outlet.branch
            and revenue_expenses.pl3 = pl_forecast_outlet.pl_section
            and date(revenue_expenses.tran_month) = pl_forecast_outlet.date_month
)

select * from final
