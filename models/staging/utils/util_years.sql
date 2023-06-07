with date_years as (
    select date_year
    from unnest(generate_date_array('2016-01-01', current_date(), interval 1 year)) as date_year
)

select * from date_years
