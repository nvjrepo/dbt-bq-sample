{% macro pl_ho_sections() -%}
ho_metrics as (
    select
        tran_week,
        tran_month,
        branch,
        'Marketing Expenses' as pl1,
        ''as pl2,
        'Accrual Brand Fund' pl3,
        - sum(amount)*0.01 as amount

    from sales_metric_grouped
    where branch != 'HCM-BG3-BC' 
    {{ dbt_utils.group_by(n=6) }}

    union all

    select
        tran_week,
        tran_month,
        'HCM-HO1' as branch,
        'Total Sales' as pl1,
        'Net Sales' as pl2,
        'Brand Fund' pl3,
        sum(amount)*0.01 as amount

    from sales_metric_grouped
    {{ dbt_utils.group_by(n=6) }}

    union all

    -- management fee is the fee charged for outlet by the HO.
    -- Since they are internal transfer, the accountant didn't book that to legers
    --BC is a separate business, so the accountant did book management fee for them
    select
        tran_week,
        tran_month,
        branch,
        'Franchiese & Management' as pl1,
        '' as pl2,
        'Management Fees' pl3,
        - sum(amount)*0.08 as amount

    from sales_metric_grouped
    where branch != 'HCM-BG3-BC' 
    {{ dbt_utils.group_by(n=6) }}

    union all

    select
        tran_week,
        tran_month,
        'HCM-HO1' as branch,
        'Total Sales' as pl1,
        'Net Sales' as pl2,
        'management_consulting' pl3,
        sum(amount)*0.08 as amount

    from sales_metric_grouped
    {{ dbt_utils.group_by(n=6) }}

),

{% endmacro -%}
