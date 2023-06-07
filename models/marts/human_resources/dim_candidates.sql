with candidates as (
    select * from {{ ref('stg_base_vn_ehiring__candidates') }}
),

final as (
    select
        candidate_id,

        apply_at,
        coalesce(coalesce(last_interview_at,offered_at),hired_at) as last_interview_at,
        coalesce(offered_at,hired_at) as offered_at,
        hired_at,
        rejected_at,

        candidate_name,
        gender,
        candidate_age,
        {{ object_segment('candidate_') }},
        recruitment_source,

        last_interview_at is not null
            or offered_at is not null
            or hired_at is not null
        as is_interview,

        offered_at is not null
            or hired_at is not null
        as is_offer,

        hired_at is not null as is_hired,
        date_trunc(hired_at,month)=date_trunc(current_date(),month) as is_new_hire
        
    
    from candidates
)

select * from final