{{
    config(
        materialized='table'
    )
}}

{%- set ops_rev=('Food','Beverage','Other') -%}
{%- set control_profits_pl1=('Direct material costs','Total COL','Operation Expenses') -%}

with ledgers as (
    select * from {{ ref('stg_ipos_accounting__ledgers') }}
),

sale_details as (
    select * from {{ ref('stg_ipos_accounting__sale_details') }}
),

vas_internal_code as (
    select * from {{ ref('stg_vas_internal_code') }}
),

items as (
    select * from {{ ref('stg_sku_items') }}
),

col_weekly as (
    select * from {{ ref('fct_weekly_col_index') }}
),

expenses as (
    select
        date_trunc(ledgers.tran_at, week(monday)) as tran_week,
        date_trunc(ledgers.tran_at, month) as tran_month,
        ledgers.branch,
        coalesce(vas_internal_code.pl_1_english, '') as pl1,
        coalesce(vas_internal_code.pl_2_english, '') as pl2,
        coalesce(vas_internal_code.internal_english, '') as pl3,
        sum(case
                when ledgers.account_id = '711' then
                    case
                        when ledgers.branch = 'HCM-HO1' then 0
                        else -ledgers.amount end
                else - ledgers.amount
            end) as amount

    from ledgers
    left join vas_internal_code
        on ledgers.expense_id = vas_internal_code.internal_code
    where true
        and (
            ledgers.account_id like '641%'
            or ledgers.account_id like '642%'
            or ledgers.account_id = '711'
            or vas_internal_code.internal_code in ('021', '022')
        )
        and ledgers.account_id_contra != '911'
    {{ dbt_utils.group_by(n=6) }}

    union all

    select
        date_trunc(timestamp(col_weekly.checking_date), week(monday)) as tran_week,
        date_trunc(timestamp(col_weekly.checking_date), month) as tran_month,
        col_weekly.outlet_code as branch,
        coalesce(vas_internal_code.pl_1_english || ' - weekly', '') as pl1,
        coalesce(vas_internal_code.pl_2_english || ' - weekly', '') as pl2,
        coalesce(vas_internal_code.internal_english || ' - weekly', '') as pl3,
        -sum(col_weekly.labour_cost) as amount

    from col_weekly
    left join vas_internal_code
        on col_weekly.expense_id = vas_internal_code.internal_code
    {{ dbt_utils.group_by(n=6) }}

),

sales_item_joined as (
    select
        date_trunc(sale_details.tran_at, week(monday)) as tran_week,
        date_trunc(sale_details.tran_at,month) as tran_month,
        sale_details.job_id as branch,
        'Total Sales' as pl1,
        'Net Sales' as pl2,
        coalesce(items.item_category, 'Other') as pl3,
        sum(sale_details.total_amount) as amount,
        - sum(sale_details.vat_tax_amount) as vat,
        - sum(sale_details.discount_amount) as revenue_deduction,
        - sum(sale_details.cog_amount) as direct_material_costs

    from sale_details
    left join items
        on sale_details.product_id = items.sku_code
    where sale_details.item_id = sale_details.product_id
    {{ dbt_utils.group_by(n=6) }}
),

{{ pl_revenue_section() }}

{{ pl_ho_sections() }}

unioned as (
    select * from sales_metric_grouped
    union all
    select * from revenue_deduction_metrics
    union all
    select * from direct_material_costs_metrics
    union all
    select * from vat_metrics
    union all
    select * from ho_metrics
    union all

    select * from expenses
    where true
        and amount < 0

)

select
    *,
    pl1 in {{ control_profits_pl1 }}
    or (
        pl1 = 'Total Sales'
        and pl3 in {{ ops_rev }}
    )
    as is_controllable_profit,
    pl1 not like '%weekly%' as is_monthly,
    coalesce(pl2, '') != 'Payroll' as is_weekly

from unioned
where branch not in ('NKKN', 'QT', 'BT', 'LQD', 'BOD', 'SH', 'FW', 'UT', '5WINE', 'LVS', 'VP', 'BEP', 'TS', 'P-KT')
