
{%- set ops_rev=('Food','Beverage','Other') -%}
{%- set date_grains=['week','month'] -%}

with revenue_expenses as (
    select * from {{ ref('int_pl_metrics_unioned') }}
    union all
    select
        tran_week,
        tran_month,
        'Total' as branch,
        pl1,
        pl2,
        pl3,
        sum(amount) as amount,
        false as is_controllable_profit,
        1 = 1 as is_monthly,
        1 = 2 as is_weekly

    from {{ ref('int_pl_metrics_unioned') }}
    where is_monthly
    {{ dbt_utils.group_by(n=6) }}
    union all
    select
        tran_week,
        tran_month,
        'Total' as branch,
        pl1,
        pl2,
        pl3,
        sum(amount) as amount,
        false as is_controllable_profit,
        1 = 2 as is_monthly,
        1 = 1 as is_weekly

    from {{ ref('int_pl_metrics_unioned') }}
    where is_weekly
    {{ dbt_utils.group_by(n=6) }}
),

{%- for date_grain in date_grains %}

net_sales_ops_metric_{{ date_grain }} as (
    select
        tran_{{ date_grain }},
        branch,
        sum(amount) as net_sales_ops_{{ date_grain }}

    from revenue_expenses
    where pl2 = 'Net Sales'
        and is_controllable_profit
    {{ dbt_utils.group_by(n=2) }}

),

{%- for ops_item in ops_rev %}
net_sales_{{ ops_item }}_metric_{{ date_grain }} as (
    select
        tran_{{ date_grain }},
        branch,
        sum(amount) as net_sales_{{ ops_item }}_{{ date_grain }}

    from revenue_expenses
    where pl2 = 'Net Sales'
        and pl3 = '{{ ops_item }}'
    {{ dbt_utils.group_by(n=2) }}

),
{% endfor -%}
{% endfor %}

final as (
        select
            {{ dbt_utils.surrogate_key(['revenue_expenses.tran_week',
                                'revenue_expenses.tran_month',
                                'revenue_expenses.branch',
                                'revenue_expenses.pl1',
                                'revenue_expenses.pl2',
                                'revenue_expenses.pl3',
                                'revenue_expenses.is_weekly',
                                'revenue_expenses.is_monthly'
                                ]) }}
            as unique_id,
            revenue_expenses.*,

            case
                {%- for ops_item in ops_rev %}    
                when revenue_expenses.pl3 = 'Theory {{ ops_item }}' then net_sales_{{ ops_item }}_metric_week.net_sales_{{ ops_item }}_week
                {%- endfor %}
                else net_sales_ops_metric_week.net_sales_ops_week
            end as net_sales_ops_week,

            case
                {%- for ops_item in ops_rev %}    
                when revenue_expenses.pl3 = 'Theory {{ ops_item }}' then net_sales_{{ ops_item }}_metric_month.net_sales_{{ ops_item }}_month
                {%- endfor %}
                else net_sales_ops_metric_month.net_sales_ops_month
            end as net_sales_ops_month

        from revenue_expenses
        
        {%- for date_grain in date_grains %}
        left join net_sales_ops_metric_{{ date_grain }}
            on revenue_expenses.tran_{{ date_grain }} = net_sales_ops_metric_{{ date_grain }}.tran_{{ date_grain }}
                and revenue_expenses.branch = net_sales_ops_metric_{{ date_grain }}.branch
        {%- for ops_item in ops_rev %}    
        left join net_sales_{{ ops_item }}_metric_{{ date_grain }}
            on revenue_expenses.tran_{{ date_grain }} = net_sales_{{ ops_item }}_metric_{{ date_grain }}.tran_{{ date_grain }}
                and revenue_expenses.branch = net_sales_{{ ops_item }}_metric_{{ date_grain }}.branch
        {% endfor %}
        {% endfor %}
    )

    select * from final
