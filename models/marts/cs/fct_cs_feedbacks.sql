with customer_feebacks as (
    select *
    from {{ ref('stg_ggsheet_customer_feedbacks') }}
),

bis_code as (
    select *
    from {{ ref('stg_gg_sheet__bis_code') }}
)

select
    customer_feebacks.*,
    --nps scoring
    if(customer_feebacks.reference_rating_score <= 6, 1, 0) as nps_detractor,
    if(customer_feebacks.reference_rating_score >= 9, 1, 0) as nps_promoters,
    if(customer_feebacks.reference_rating_score between 7 and 8, 1, 0) as nps_passives,
    bis_code.bu_1,
    bis_code.outlet_code,
    bis_code.outlet_name,
    bis_code.accessed_email

from customer_feebacks
left join bis_code
          on lower(customer_feebacks.branch) = lower(bis_code.outlet_name)
