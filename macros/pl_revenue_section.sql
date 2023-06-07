{% macro pl_revenue_section() -%}
direct_material_costs_metrics as (
    select
        tran_week,
        tran_month,
        branch,
        'Direct material costs' as pl1,
        'theoritical_cost' as pl2,
        concat('Theory ',pl3) as pl3,
        direct_material_costs as amount

    from sales_item_joined

),

vat_metrics as (
    select
        tran_week,
        tran_month,
        branch,
        'Total Sales' as pl1,
        'vat' as pl2,
        '' as pl3,
        sum(vat) as amount

    from sales_item_joined
    {{ dbt_utils.group_by(n=6) }}

),

revenue_deduction_metrics as (
    select
        tran_week,
        tran_month,
        branch,
        'Total Sales' as pl1,
        'revenue_deduction' as pl2,
        '' as pl3,
        sum(revenue_deduction) as amount

    from sales_item_joined
    {{ dbt_utils.group_by(n=6) }}

),

sales_metrics_raw as (
    select
        tran_week,
        tran_month,
        branch,
        'Total Sales' as pl1,
        'Net Sales' as pl2,
        pl3,
        amount

    from sales_item_joined

    union all

    select
        tran_week,
        tran_month,
        branch,
        pl1,
        pl2,
        pl3,
        amount

    from expenses
    where amount > 0
),

sales_metric_grouped as (
    select
        tran_week,
        tran_month,
        branch,
        pl1,
        pl2,
        pl3,
        sum(amount) as amount

    from sales_metrics_raw
    {{ dbt_utils.group_by(n=6) }}
),

{% endmacro -%}