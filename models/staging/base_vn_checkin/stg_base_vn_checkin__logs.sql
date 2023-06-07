{%- set schemas_to_convert=[
    'stats_comments',
    'logs_client_id',
    'logs_content',
    'logs_img',
    'logs_lat',
    'logs_lng',
    'logs_note',
    'logs_photo'
]-%}

with sources as (
    select * from {{ source('base_checkin', 'checkin_logs') }}
),

renamed as (
    select distinct
        --id
        {{ dbt_utils.surrogate_key(['id','logs_time','employee_id']) }} as unique_id,
        id as checkin_log_id,
        user_id,
        employee_id,
        timesheet_id,
        hid,

        --time
        datetime(timestamp_seconds(cast(logs_time as int64)),'+07') as checkin_at,
        datetime(timestamp_seconds(cast(date as int64)),'+07') as checkin_date,
        datetime(timestamp_seconds(cast(month_index as int64)),'+07') as checkin_month,
        loaded_date,

        token as checkin_token,
        type as data_type,

        --finalized
        finalized_is_late=1 as is_late,
        finalized_day_point as day_point,
        finalized_sum_minute_late as minutes_late,
        finalized_sum_late as late,
        finalized_shift_info as shift_info,

        --log information
        logs_ip,
        logs_checkout,
        logs_files,
        logs_metatype,
        {% for schema_name in schemas_to_convert -%}
            {{ convert_string_to_null(schema_name) }}{{ ',' if not loop.last }}
        {% endfor %}

        --computed
        --computed_is_late,
        --computed_day_point,
        --computed_sum_minute_late,
        --computed_sum_late,
        --computed_shift_info,

    from sources

)

select * from renamed