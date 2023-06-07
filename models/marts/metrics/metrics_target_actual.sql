with order_line_metrics as (
    select * from {{ ref('int_order_line_metrics') }}
),

demand_supply_target as (
    select * from {{ ref('fct_demand_supply_target') }}
),

order_metrics as (
    select * from {{ ref('int_order_metrics') }}
),

targets as (
    select * from {{ ref('int_targets') }}
),

sale_report_index as (
    select * from {{ ref('sale_report_index') }}
),

order_metrics_grouped as (
    select
        tran_at,
        outlet_code,
        sum(case when metric_names = 'customer' then metric_value end) as actual_customers,
        sum(case when metric_names = 'booking_pax' then metric_value end) as actual_booking_pax

    from order_metrics
    {{ dbt_utils.group_by(n=2) }}
),

unioned as (
    select
        date(order_line_metrics.tran_at) as date_day,
        order_line_metrics.outlet_code,
        order_line_metrics.metric_names,
        targets.metric_value as target_value,
        coalesce(order_line_metrics.metric_value,0) as actual_value

    from order_line_metrics
    left join targets
        on targets.outlet_code = order_line_metrics.outlet_code
            and targets.date_day = date(order_line_metrics.tran_at)
            and targets.metric_names = order_line_metrics.metric_names

    union all

    select
        tran_day as date_day,
        outlet_code,
        'abi_vol_outlet' as metric_names,
        sum(target_demand_quantity) as target_value,
        sum(actual_demand_quantity_converted) as actual_value

    from demand_supply_target
    where supplier = 'ABI'
    {{ dbt_utils.group_by(n=3) }}

    union all    
    --metrics that do not have target
    select
        date(tran_at) as date_day,
        outlet_code,
        metric_names,
        null as target_value,
        metric_value as actual_value

    from order_metrics

),

granularity as (
    select
        {{ dbt_utils.surrogate_key(['unioned.outlet_code','unioned.date_day','unioned.metric_names']) }} as unique_id,
        unioned.*,
        sum(actual_value) over (partition by unioned.outlet_code, unioned.metric_names, date_trunc(unioned.date_day,month)) as monthly_actual_value,
        date_trunc(unioned.date_day, week) as date_week,
        date_trunc(unioned.date_day, month) as date_month,
        coalesce(targets.metric_value,0) as target_net_sales,
        coalesce(order_line_metrics.metric_value,0) as actual_net_sales,
        coalesce(order_metrics_grouped.actual_booking_pax,0) as actual_booking_pax,
        coalesce(order_metrics_grouped.actual_customers,0) as actual_customers,
        sale_report_index.index
    
    from unioned
    left join sale_report_index
        on unioned.metric_names = sale_report_index.metric_names
    left join targets
        on unioned.outlet_code = targets.outlet_code
            and unioned.date_day = date(targets.date_day)
            and targets.metric_names = 'net_sales'
    left join order_line_metrics
        on unioned.outlet_code = order_line_metrics.outlet_code
            and unioned.date_day = date(order_line_metrics.tran_at)
            and order_line_metrics.metric_names = 'net_sales'
    left join order_metrics_grouped
        on unioned.outlet_code = order_metrics_grouped.outlet_code
            and unioned.date_day = date(order_metrics_grouped.tran_at)

)

select * from granularity