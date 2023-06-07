with date_days as (
    select
        date_day,
        date_trunc(date_day, month) as date_month,
        extract(dayofweek from date_day) as day_of_week

    from unnest(generate_date_array('2016-01-01', current_date(), interval 1 day)) as date_day
)

select * from date_days