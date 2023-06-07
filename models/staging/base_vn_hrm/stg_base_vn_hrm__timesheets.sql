with sources as (
    select * from {{ source('base_hrm', 'timesheet') }}
),

renamed as (
    select 
        --id
        id as timesheet_id,
        user_id,
        {{ convert_string_to_null('df') }},

        --time
        datetime(timestamp_seconds(cast(since as int64)), '+07') as since_at,
        datetime(timestamp_seconds(cast(last_update as int64)), '+07') as last_update_at,
        loaded_date,

        --dimension
        code as timesheet_code,
        name as timesheet_name,
        type as timesheet_type,
        status as timesheet_status,

        --shift
        cast(num_shifts as int64) as num_shifts,
        json_value_array(working_days) as working_days,
        shifts,

        --per day
        cast(points_per_day as float64) as points_per_day,
        cast(hours_per_day as float64) as hours_per_day,

        --config
        config_type,
        cast(config_checkin_gap as int64) as config_checkin_gap,
        cast(config_checkout_gap as int64) as config_checkout_gap,
        cast(config_required_checkout as int64) as config_required_checkout,
        cast(config_eba as int64) as config_eba,

        --other
        cast(timezone as int64) as timezone,
        eba,
        json_value_array(owners) as owners,
        metatype

    from sources
)

select * from renamed






