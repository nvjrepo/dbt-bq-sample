with date_weeks as (
    select date_week
    from unnest(generate_date_array('2015-12-28', current_date(), interval 1 week)) as date_week
)

select * from date_weeks