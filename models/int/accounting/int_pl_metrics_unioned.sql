{{
    config(
        materialized='table'
    )
}}

{%- set date_grains=['week','month'] -%}
{%- set date_grains_r=['month','week'] -%}
{%- set metric_ctes=['controlable_profit_metrics',
                      'profit_before_taxes_metrics',
                      'cit_metrics',
                      'net_profit_metrics',
                      'ebit_metrics',
                      'ebitda_metrics'
] -%}

with revenue_expenses as (
    select * from {{ ref('int_revenue_expenses') }}
),

{% for date_grain,date_grain_r in zip(date_grains,date_grains_r) %}
    --profit_before_taxes
    controlable_profit_metrics_{{ date_grain }} as (
        select
            timestamp(null) as tran_{{ date_grain_r }},
            tran_{{ date_grain }},
            branch,
            'Controlable Profit' as pl1,
            sum(amount) as amount

        from revenue_expenses
        where is_controllable_profit
            and is_{{ date_grain }}ly
        {{ dbt_utils.group_by(n=4) }}
    ),

    --profit_before_taxes
    profit_before_taxes_metrics_{{ date_grain }} as (
        select
            timestamp(null) as tran_{{ date_grain_r }},
            tran_{{ date_grain }},
            branch,
            'Profit Before Taxes' as pl1,
            sum(amount) as amount

        from revenue_expenses
        where pl1 is not null
            and is_{{ date_grain }}ly
        {{ dbt_utils.group_by(n=4) }}
    ),

    --cit
    cit_metrics_{{ date_grain }} as (
        select
            timestamp(null) as tran_{{ date_grain_r }},
            tran_{{ date_grain }},
            branch,
            'CIT' as pl1,
            case when amount > 0 then -amount * 0.2 else 0 end as amount

        from profit_before_taxes_metrics_{{ date_grain }}
    ),

    --net_profit
    int_net_profit_{{ date_grain }} as (
        select * from profit_before_taxes_metrics_{{ date_grain }}
        union all
        select * from cit_metrics_{{ date_grain }}
    ),

    net_profit_metrics_{{ date_grain }} as (
        select
            timestamp(null) as tran_{{ date_grain_r }},
            tran_{{ date_grain }},
            branch,
            'Net Profit' as pl1,
            sum(amount) as amount

        from int_net_profit_{{ date_grain }}
        {{ dbt_utils.group_by(n=4) }}

    ),

    --EBIT&EBITDA
    int_profit_ida_metrics_{{ date_grain }} as (
        select
            timestamp(null) as tran_{{ date_grain_r }},
            tran_{{ date_grain }},
            branch,
            pl1,
            amount

        from profit_before_taxes_metrics_{{ date_grain }}
        union all

        select
            timestamp(null) as tran_{{ date_grain_r }},
            tran_{{ date_grain }},
            branch,
            pl1,
            amount

        from revenue_expenses
        where pl1 in ('D&A', 'Financial Fees')
            and is_{{ date_grain }}ly

    ),

    --EBIT
    ebit_metrics_{{ date_grain }} as (
        select
            timestamp(null) as tran_{{ date_grain_r }},
            tran_{{ date_grain }},
            branch,
            'EBIT' as pl1,
            sum(amount) as amount

        from int_profit_ida_metrics_{{ date_grain }}
        where pl1 in ('Profit Before Taxes', 'Financial Fees')
        {{ dbt_utils.group_by(n=4) }}
    ),

    --EBITDA
    ebitda_metrics_{{ date_grain }} as (
        select
            timestamp(null) as tran_{{ date_grain_r }},
            tran_{{ date_grain }},
            branch,
            'EBITDA' as pl1,
            sum(amount) as amount

        from int_profit_ida_metrics_{{ date_grain }}
        where pl1 in ('Profit Before Taxes', 'D&A')
        {{ dbt_utils.group_by(n=4) }}

    ),
{% endfor %}

unioned as (
{%- for date_grain in date_grains %}
    {%- for metric_cte in metric_ctes %}
        select
            tran_week,
            tran_month,
            pl1,
            branch,
            amount
        from {{ metric_cte }}_{{ date_grain }}
        {{ 'union all' if not loop.last -}}
    {% endfor -%}
    {{ 'union all' if not loop.last -}}
{% endfor %}
),

final as (
    select * from revenue_expenses
    union all
    select
        tran_week,
        tran_month,
        branch,
        pl1,
        '' as pl2,
        '' as pl3,
        amount,
        pl1 = 'Controlable Profit' as is_controllable_profit,

        tran_month is not null as is_monthly,
        tran_week is not null as is_weekly

    from unioned
)

select * from final
