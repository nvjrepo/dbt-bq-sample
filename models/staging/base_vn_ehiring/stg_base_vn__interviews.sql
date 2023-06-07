with sources as (
    select * from {{ source('base_ehiring', 'interview') }}
),

renamed as (
    select
        --id
        id as interview_id,
        user_id,
        msgthread_id,
        opening_id,

        --timestamp       
        datetime(timestamp_seconds(cast(`time` as int64)),'+07') as interview_at,
        datetime(timestamp_seconds(`date`),'+07') as interview_date,
        loaded_date	TIMESTAMP,

        --interview
        `name` as interview_name,
        status as interview_status,
        opening_name,

        {{ convert_string_to_null('location',adds='candidate_') }},
        {{ convert_string_to_null('questions',adds='candidate_') }},
        est_duration as duration,

        --candidate
        candidate_id,
        candidate_name,
        candidate_phone,
        candidate_email,
        candidate_title,

        --candidate_export
        candidate_export_id,
        candidate_export_name,
        candidate_export_type,
        candidate_export_phone,
        candidate_export_email,
        candidate_export_title,
        candidate_export_link,

        --acl
        acl_view,
        acl_managed,
        acl,

        --boolean
        confirmed as is_confirmed,
        interacted as is_interacted,
        hr_set_cf as is_hr_set_cf,

        --system
        username,
        metatype,
        token,

        --others
        starred,
        last_ping,
        followers,
        sendmail as send_mail

    from sources
    where `name` != 'test'

)

select * from renamed
