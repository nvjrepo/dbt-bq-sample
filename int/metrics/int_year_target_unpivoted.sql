with yearly_target_unpivoted as  (
    {{ dbt_utils.unpivot(ref('stg_gg_sheet__yearly_targets'), 
       cast_to='float64',
       field_name='date_month',
       value_name='metric_value', 
       exclude=['unique_id','metric_names','outlet_code','target_year']) 
    }}
),

yearly_targets as (
    select
        metric_names,
        outlet_code,
        date(
            concat(
                target_year,
                '-',
                replace(date_month,'m_',''),
                '-01'
            ) 
        ) as date_month,
        metric_value
    
    from yearly_target_unpivoted
)

select * from yearly_targets