{{
    config (
        enabled = false
    )
}}

{%- set weeks = [1,2,3,4,5] -%}

with acc_warehouses as (
    select
        *,
        extract(month from date_trunc(tran_at,month)) = 7 as is_current_month,
        extract(month from date_trunc(tran_at,month)) = 6 as is_last_month,
        extract(month from date_trunc(tran_at,month)) = 5 as is_last_2_month,
        date(date_trunc(tran_at,month)) between date_add(date_trunc(current_date(),month), interval -2 month)
                                        and case 
                                                when extract (dayofweek from last_day(date_add(current_date(),interval -2 month),month)) = 1
                                                    then last_day(date_add(current_date(),interval -2 month),month)
                                                else  date_add(date_trunc(last_day(date_add(current_date(),interval -2 month),month), week(monday)), interval - 1 day)
                                            end
        as is_last_whole_week_month_calculation

    from {{ ref('stg_ipos_accounting__warehouses') }}
),

acc_warehouse_balances as (
    select
        *,
        extract(month from date_trunc(tran_at,month)) = 7 as is_current_month,
        extract(month from date_trunc(tran_at,month)) = 6 as is_last_month,
        extract(month from date_trunc(tran_at,month)) = 5 as is_last_2_month,
        date(date_trunc(tran_at,month)) between date_add(date_trunc(current_date(),month), interval -2 month)
                                        and case 
                                                when extract (dayofweek from last_day(date_add(current_date(),interval -2 month),month)) = 1
                                                    then last_day(date_add(current_date(),interval -2 month),month)
                                                else  date_add(date_trunc(last_day(date_add(current_date(),interval -2 month),month), week(monday)), interval - 1 day)
                                            end
        as is_last_whole_week_month_calculation

    from {{ ref('stg_ipos_accounting__warehouse_balances') }}
),

------------------------------

stock_movement_last_period as (
    select
        warehouse_id,
        item_id,
        sum(case when tran_id like 'N%' then quantity end) as in_quantity,
        sum(case when tran_id not like 'N%' then quantity end) as out_quantity

    from acc_warehouses
    where is_last_whole_week_month_calculation
    {{ dbt_utils.group_by(n=2) }}
),

stock_start_balance_last_period as (
    select * from acc_warehouse_balances
    where is_last_2_month
),

week_start_balane_metric as (
    select
        coalesce(stock_start_balance_last_period.warehouse_id,stock_movement_last_period.warehouse_id) as warehouse_id,
        coalesce(stock_start_balance_last_period.item_id,stock_movement_last_period.item_id) as item_id,
        coalesce(stock_start_balance_last_period.quantity,0)
        + coalesce(stock_movement_last_period.in_quantity,0)
        - coalesce(stock_movement_last_period.out_quantity,0)     
        as week_start_balane

    from stock_start_balance_last_period
    full join stock_movement_last_period
        on stock_start_balance_last_period.warehouse_id = stock_movement_last_period.warehouse_id
            and stock_start_balance_last_period.item_id = stock_movement_last_period.item_id

),

--------------------------------

acc_warehouses_this_period as (
    select
        date_trunc(tran_at,week(monday)) as tran_week,
        warehouse_id,
        item_id,
        sum(case when tran_id like 'N%' then quantity end)
        - sum(case when tran_id not like 'N%' then quantity end) 
        as in_out_quantity

    from acc_warehouses
    where is_current_month
    {{ dbt_utils.group_by(n=3) }}
),

acc_warehouse_balances_this_period as (
    select * from acc_warehouse_balances
    where is_last_month
),

week_order as (
    select
        date_week,
        row_number() over (order by date_week ) as week_no    
    from {{ ref('util_weeks') }}
    where date_week >= date(date_trunc(date_trunc(current_date(),month),week(monday)))
),

{% for week_no in weeks -%}

week_{{ week_no }}_movement as (
    select
        acc_warehouses_this_period.*
    
    from acc_warehouses_this_period
    inner join week_order
        on date(acc_warehouses_this_period.tran_week) = week_order.date_week
    where week_order.week_no = {{ week_no }}

),

{% endfor -%}

final as (
    select
        coalesce(acc_warehouse_balances_this_period.warehouse_id,
                a1.warehouse_id,
                a2.warehouse_id,
                a3.warehouse_id,
                a4.warehouse_id,
                a5.warehouse_id,
                week_start_balane_metric.warehouse_id) 
        as outlet_code,
        coalesce(acc_warehouse_balances_this_period.item_id,
                a1.item_id,
                a2.item_id,
                a3.item_id,
                a4.item_id,
                a5.item_id,
                week_start_balane_metric.item_id) 
        as item_id,
        sum(coalesce(acc_warehouse_balances_this_period.quantity,0)) as month_ending_quantity,
        
        sum(
            coalesce(week_start_balane_metric.week_start_balane,0)
            + coalesce(a1.in_out_quantity,0)
        )
        as week_1_ending_quantity,

        sum(
            coalesce(week_start_balane_metric.week_start_balane,0)
            + coalesce(a1.in_out_quantity,0)  
            + coalesce(a2.in_out_quantity,0)
        )
        as week_2_ending_quantity,

        sum(
            coalesce(week_start_balane_metric.week_start_balane,0)
            + coalesce(a1.in_out_quantity,0)
            + coalesce(a2.in_out_quantity,0)
            + coalesce(a3.in_out_quantity,0)
        )
        as week_3_ending_quantity,

        sum(
            coalesce(week_start_balane_metric.week_start_balane,0)
            + coalesce(a1.in_out_quantity,0)  
            + coalesce(a2.in_out_quantity,0)
            + coalesce(a3.in_out_quantity,0)
            + coalesce(a4.in_out_quantity,0)
        )
        as week_4_ending_quantity,

        sum(
            coalesce(week_start_balane_metric.week_start_balane,0)
            + coalesce(a1.in_out_quantity,0) 
            + coalesce(a2.in_out_quantity,0)
            + coalesce(a3.in_out_quantity,0) 
            + coalesce(a4.in_out_quantity,0)
            + coalesce(a5.in_out_quantity,0)
        )
        as week_5_ending_quantity

    from acc_warehouse_balances_this_period

    {% for week_no in weeks -%}
    full join week_{{ week_no }}_movement as a{{ week_no }}
        on acc_warehouse_balances_this_period.warehouse_id = a{{ week_no }}.warehouse_id
            and acc_warehouse_balances_this_period.item_id = a{{ week_no }}.item_id
    {% endfor -%}

    full join week_start_balane_metric
        on acc_warehouse_balances_this_period.warehouse_id = week_start_balane_metric.warehouse_id
            and acc_warehouse_balances_this_period.item_id = week_start_balane_metric.item_id
    {{ dbt_utils.group_by(n=2) }}
)

select * from final
