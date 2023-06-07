
actual_supply_fixed as (
    select
        outlet_code,
        item_id,
        sum(case when date(date_trunc(tran_day,month)) = date'2022-07-01' then actual_supply_quantity end) as actual_supply_month,
        sum(case when date(date_trunc(tran_day,week(monday))) = date'2022-06-27' then actual_supply_quantity end) as actual_supply_week1,
        sum(case when date(date_trunc(tran_day,week(monday))) = date'2022-07-04' then actual_supply_quantity end) as actual_supply_week2,
        sum(case when date(date_trunc(tran_day,week(monday))) = date'2022-07-11' then actual_supply_quantity end) as actual_supply_week3,
        sum(case when date(date_trunc(tran_day,week(monday))) = date'2022-07-18' then actual_supply_quantity end) as actual_supply_week4,
        sum(case when date(date_trunc(tran_day,week(monday))) = date'2022-07-25' then actual_supply_quantity end) as actual_supply_week5
    
    from actual_supply_metric
    {{ dbt_utils.group_by(n=2) }}
),

actual_demand_fixed as (
    select
        outlet_code,
        item_id,
        sum(case when date(date_trunc(tran_day,month)) = '2022-07-01' then actual_demand_quantity end) as actual_demand_month,
        sum(case when date(date_trunc(tran_day,week(monday))) = date'2022-06-27' then actual_demand_quantity end) as actual_demand_week1,
        sum(case when date(date_trunc(tran_day,week(monday))) = date'2022-07-04' then actual_demand_quantity end) as actual_demand_week2,
        sum(case when date(date_trunc(tran_day,week(monday))) = date'2022-07-11' then actual_demand_quantity end) as actual_demand_week3,
        sum(case when date(date_trunc(tran_day,week(monday))) = date'2022-07-18' then actual_demand_quantity end) as actual_demand_week4,
        sum(case when date(date_trunc(tran_day,week(monday))) = date'2022-07-25' then actual_demand_quantity end) as actual_demand_week5
    
    from demand_quantity_metric
    {{ dbt_utils.group_by(n=2) }}
),


joined_2 as (
    select
        joined_1.*,
        actual_demand_fixed.actual_demand_month,
        actual_demand_fixed.actual_demand_week1,
        actual_demand_fixed.actual_demand_week2,
        actual_demand_fixed.actual_demand_week3,
        actual_demand_fixed.actual_demand_week4,
        actual_demand_fixed.actual_demand_week5,
        actual_supply_fixed.actual_supply_month,
        actual_supply_fixed.actual_supply_week1,
        actual_supply_fixed.actual_supply_week2,
        actual_supply_fixed.actual_supply_week3,
        actual_supply_fixed.actual_supply_week4,
        actual_supply_fixed.actual_supply_week5
    
    from joined_1
    left join actual_demand_fixed
        on joined_1.outlet_code = actual_demand_fixed.outlet_code
            and joined_1.item_id = actual_demand_fixed.item_id

    left join actual_supply_fixed
        on joined_1.outlet_code = actual_supply_fixed.outlet_code
            and joined_1.item_id = actual_supply_fixed.item_id     

)
