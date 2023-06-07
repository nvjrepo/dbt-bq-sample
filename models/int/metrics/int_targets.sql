with date_series as (
    select * from {{ ref('util_days') }}
),

yearly_targets as (
    select * from {{ ref('int_year_target_unpivoted') }}
),

daily_targets as (
    select
        forecasted_date as date_day,
        outlet_code,
        'net_sales' as metric_names,
        forecasted_net_sales as metric_value,
        1 as orders

    from {{ ref('stg_ggsheet__daily_sale_targets') }}
),

target_unioned as (
    select
        date_series.date_day,
        yearly_targets.outlet_code,
        yearly_targets.metric_names,
        case
            when yearly_targets.metric_names like '%percent' then yearly_targets.metric_value
            else coalesce(yearly_targets.metric_value / extract(day from last_day(yearly_targets.date_month)),0)
        end as metric_value,
        2 as orders

    from yearly_targets
    inner join date_series
        on date(date_trunc(date_series.date_day, month)) = yearly_targets.date_month

    union all

    select * from daily_targets

),

cog_metric as (
    select
        t1.date_day,
        t1.outlet_code,
        'cog' as metric_names,
        (case when t1.metric_names = 'net_sales' then t1.metric_value end)
        * (case when t2.metric_names = 'cog_percent' then t2.metric_value end)
        as metric_value,
        1 as orders

    from target_unioned as t1
    left join target_unioned as t2
        on t1.date_day = t2.date_day
            and t1.outlet_code = t2.outlet_code
            and t2.metric_names = 'cog_percent'
    where t1.metric_names = 'net_sales'

),

col_metric as (
    select
        t1.date_day,
        t1.outlet_code,
        'col' as metric_names,
        (case when t1.metric_names = 'net_sales' then t1.metric_value end)
        * (case when t2.metric_names = 'col_percent' then t2.metric_value end)
        as metric_value,
        1 as orders

    from target_unioned as t1
    left join target_unioned as t2
        on t1.date_day = t2.date_day
            and t1.outlet_code = t2.outlet_code
            and t2.metric_names = 'col_percent'
    where t1.metric_names = 'net_sales'

),

unioned_1 as (
    select * from target_unioned
    where metric_names not like '%percent'
    union all
    select * from cog_metric
    union all
    select * from col_metric
),

gm_metric as (
    select
        t1.date_day,
        t1.outlet_code,
        'gross_margin' as metric_names,
        t1.metric_value - t2.metric_value as metric_value,
        1 as orders

    from unioned_1 as t1
    left join unioned_1 as t2
        on t1.date_day = t2.date_day
            and t1.outlet_code = t2.outlet_code
            and t2.metric_names = 'cog'
    where t1.metric_names = 'net_sales'

),

unioned_2 as (
    select * from unioned_1
    union all
    select * from gm_metric
),

final as (
    select
        *,
        {{ dbt_utils.surrogate_key(['outlet_code','date_day','metric_names']) }} as unique_id
    from unioned_2
    where date_day <= date_sub(current_date(), interval 1 day)
)

select * from final
qualify row_number() over (partition by unique_id order by orders) = 1
