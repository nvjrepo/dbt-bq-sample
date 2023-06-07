{{
    config(
        enabled=false
    )
}}
with candidates as (
    select * from {{ ref('stg_base_vn_ehiring__candidates') }}
),

openings as (
    select * from {{ ref('stg_base_vn_ehiring__openings') }}
),

joined as (
    select
        candidates.candidate_id,
        candidates.candidate_phone,
        candidates.candidate_email,
        candidates.candidate_name,
        candidates.dob,
        (openings.salary_min+openings.salary_max)/2 as avg_salary

    from candidates
    left join openings
        on candidates.opening_id = openings.opening_id
)

select * from joined