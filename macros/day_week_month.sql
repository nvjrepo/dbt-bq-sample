{% macro day_week_month(date_at) %}

        --date(date_trunc(current_date(),month)) as first_day_current_month,
        --date(date_add(date_trunc(current_date(),month), interval -1 month)) as first_day_last_month,
        --date(last_day(date_add(current_date(),interval -1 month),month)) as last_day_last_month,
        --case 
        --    when extract (dayofweek from last_day(date_add(current_date(),interval -1 month),month)) = 1
        --        then last_day(date_add(current_date(),interval -1 month),month)
        --    else  date_add(date_trunc(last_day(date_add(current_date(),interval -1 month),month), week(monday)), interval - 1 day)
        --end as last_day_last_week_last_month
        --date(date_add(date_trunc(current_date(),month), interval -2 month)) as first_day_last_2_month,
        --date(date_trunc(date_trunc(current_date(),month),week(monday))) as first_week_current_month,

        date(date_trunc({{ date_at }},month)) = date(date_trunc(current_date(),month)) as is_current_month,
        date(date_trunc({{ date_at }},month)) = date_add(date_trunc(current_date(),month), interval -1 month) as is_last_month,
        date(date_trunc({{ date_at }},month)) = date_add(date_trunc(current_date(),month), interval -2 month) as is_last_2_month,
        date(date_trunc({{ date_at }},month)) between date_add(date_trunc(current_date(),month), interval -1 month)
                                        and case 
                                                when extract (dayofweek from last_day(date_add(current_date(),interval -1 month),month)) = 1
                                                    then last_day(date_add(current_date(),interval -1 month),month)
                                                else  date_add(date_trunc(last_day(date_add(current_date(),interval -1 month),month), week(monday)), interval - 1 day)
                                            end
        as is_last_whole_week_month_calculation

{% endmacro %}