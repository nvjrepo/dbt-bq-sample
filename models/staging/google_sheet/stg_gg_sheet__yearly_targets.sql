with past_targets as (
    select * from {{ source('past_target','yearly_target') }}
),

current_targets as (
    select * from {{ ref('base_ggsheet__current_yearly_target') }}
),

unioned as (
    select * from past_targets
    union all
    select * from current_targets

)

select * from unioned
