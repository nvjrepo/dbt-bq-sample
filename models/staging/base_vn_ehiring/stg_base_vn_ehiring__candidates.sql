{%- set time_dimensions=['apply',
                         'offered',
                         'hired',
                         'rejected',
                         'campaign_qualified'
]
-%}

with sources as (
    select * from {{ source('base_ehiring', 'candidate') }}
),

renamed as (
    select
        --id
        id as candidate_id,
        opening_export_id,
        reason_id,
        stage_id,
        opening_id,
        
        --candidate pii
        {{ convert_string_to_null('name',adds='candidate_') }},
        {{ convert_string_to_null('first_name') }},
        {{ convert_string_to_null('last_name') }},
        email as candidate_email,
        case when left(phone,1)!='0' then concat('0',phone) else phone end as candidate_phone,
        dob_day,
        dob_month,
        dob_year,
        concat(dob_day,dob_month,dob_year) as dob,
        gender,
        {{ convert_string_to_null('address',adds='candidate_') }},

        --time
        {% for time_dimension in time_dimensions -%}
            case 
                when time_{{ time_dimension }}='0' then null
                else datetime(timestamp_seconds(cast(time_{{ time_dimension }} as int64)),'+07')
            end as {{ time_dimension }}_at,
        {% endfor %}
        case 
            when last_interview='0' then null
            else datetime(timestamp_seconds(cast(last_interview as int64)),'+07')
        end as last_interview_at,
        datetime(timestamp_seconds(cast(last_update as int64)),'+07') as last_update_at,
        datetime(timestamp_seconds(cast(last_time_stage as int64)),'+07') as last_stage_at,
        datetime(timestamp_seconds(cast(since as int64)),'+07') as since,
        loaded_date,


        --application information
        `status` as candidate_status,
        stage_name,
        {{ convert_string_to_null('title') }},
        display_title,
        score,
        evaluations,
        tags,

        --other information
        source as recruitment_source,
        metatype,
        {{ convert_string_to_null('campaign') }},
        {{ convert_string_to_null('reason') }},

        --interview
        interview_count,
        {{ convert_string_to_null('last_stage_before_failed') }},

        --system
        disp_name,
        cvs,
        fields,
        form,
        {{ convert_string_to_null('prefix') }},
        {{ convert_string_to_null('ssn') }},
        {{ convert_string_to_null('medium') }},
        assign_username,

        --opening
        opening_export_name,
        opening_export_type,
        opening_export_abslink,
        opening_export_status_display,
        opening_export_codename,
        opening_export_owners,
        opening_export_creator,
        opening_export,

        extract(year from current_date()) 
            - (case when dob_year='1900' then 0 else cast(dob_year as int64) end) as candidate_age,

    from sources

)

select * from renamed
