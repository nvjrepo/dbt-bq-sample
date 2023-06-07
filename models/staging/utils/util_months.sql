with date_months as (
    select date_month
    from unnest(generate_date_array('2016-01-01', current_date(), interval 1 month)) as date_month
)

select * from date_months