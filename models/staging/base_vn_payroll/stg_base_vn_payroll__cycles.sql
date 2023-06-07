with sources as  (
    select * from {{ source('basevn_payroll','cycle') }}
),

renamed as (
    select
        --primary key
        id as cycle_id,

        --user info
        user_id,
        username,

        --time
        report_year,
        datetime(timestamp_seconds(cast(start_date as int64)),'+07') started_at,
        datetime(timestamp_seconds(cast(end_date as int64)),'+07') ended_at,
        datetime(timestamp_seconds(cast(report_month as int64)),'+07') report_month,
        datetime(timestamp_seconds(cast(since as int64)),'+07') since,
        datetime(timestamp_seconds(cast(last_update as int64)),'+07') last_updated_at,
        loaded_date as loaded_at,
        
        --other
        schedule,
        system_id

    from sources
)

select * from renamed
