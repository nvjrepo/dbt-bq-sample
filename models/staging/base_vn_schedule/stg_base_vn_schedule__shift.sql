with sources as (
    select * from {{ source('basevn_schedule','shift') }}
),

renamed as (
    select
        --primary key
        id as unique_id,
        --timesheet id
        timesheet_id,
        employee_id,
        --others key
        user_id,
        creator_id,
        type_id,
        jobsite_id,

        --dim
        slot,
        color,
        note,
        type_name,
        jobsite_name,

        --boolean
        published = '1' as is_published,
        --time

        datetime(timestamp_seconds(cast(date as int64)), '+07') as date,
        datetime(timestamp_seconds(cast(s_time as int64)), '+07') as start_at,
        datetime(timestamp_seconds(cast(e_time as int64)), '+07') as end_at,
        datetime(timestamp_seconds(cast(since as int64)), '+07') as since_at,
        datetime(timestamp_seconds(cast(last_update as int64)), '+07') as last_update_at,
        loaded_date as loaded_at,

        --other
        earliest_ci,
        latest_ci,
        earliest_co,
        latest_co,
        break,
        standard_point

    from sources
)

select * from renamed
