with pl_metrics as (
    select * from {{ ref('int_pl_net_sales_segment_joined') }}
),

bis_code as (
    select * from {{ ref('stg_gg_sheet__bis_code') }}
),

pl_report_index as (
    select * from {{ ref('pl_report_index') }}
),

final as (
    select
        pl_metrics.*,
        bis_code.bu_1 as bu,
        pl_report_index.index,
        bis_code.bu_1 in ('BAGAC', 'FIREWORK')
        and pl_metrics.is_controllable_profit
        as is_operation,

        case
            when pl_metrics.pl1 in ('Total Sales', 'Financial Income', 'Financial Income') then 'Revenue'
            when pl_metrics.pl1 in ('Controlable Profit', 'Profit Before Taxes', 'Net Profit', 'EBIT', 'EBITDA') then 'Summary'
            else 'Expense'
        end as pl_segment


    from pl_metrics
    left join bis_code
        on pl_metrics.branch = bis_code.outlet_code
    left join pl_report_index
        on pl_metrics.pl1 = pl_report_index.item_name
    where pl_report_index.index is not null

)

select
    *,
    if(is_monthly, sum(amount) over (partition by tran_month,pl1,pl2,is_operation), 0) as amount_cuml_month,
    if(is_monthly, sum(amount) over (partition by branch,pl1,pl2,is_operation), 0) as amount_cuml_outlet_is_monthly,

    if(is_weekly, sum(amount) over (partition by tran_week,pl1,pl2,is_operation), 0) as amount_cuml_weekly,
    if(is_weekly, sum(amount) over (partition by branch,pl1,pl2,is_operation), 0) as amount_cuml_outlet_is_weekly

from final
