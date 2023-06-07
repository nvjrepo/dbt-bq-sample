with sales as (
    select * from {{ ref('int_sales') }}
),

bis_code as (
    select * from {{ ref('stg_gg_sheet__bis_code') }}
),

campaigns as (
    select * from {{ ref('stg_ipos_crm__campaigns') }}
),

sale_logic as (
    select
        *,

        case
            when extract(hour from time_in) >= 7 and extract(hour from time_in) <= 14 then 'Morning'
            else 'Evening'
        end as shift,

        case
            when extract(hour from time_in) >= 6 and extract(hour from time_in) <= 10 then '1 Breakfast'
            when extract(hour from time_in) >= 11 and extract(hour from time_in) <= 14 then '2 Lunch'
            when extract(hour from time_in) >= 15 and extract(hour from time_in) <= 18 then '3 Happy Hours'
            when extract(hour from time_in) >= 19 and extract(hour from time_in) <= 21 then '4 Dinner'
            else '5 Late Night'
        end as session_round,

        case
            when lower(delivery_party_name) in (
                'grab',
                'goviet',
                'now',
                'pos_take away',
                'baemin'
            )
            then 'Home delivery'
            else 'Dine in'
        end as group_channel,

        row_number() over (partition by membership_id order by tran_at asc, tran_hour asc) as member_order_number_orginal,
        lag(tran_day) over (partition by membership_id order by tran_at asc, tran_hour asc) as member_pre_order_day, ---to detect if this is the bill splited at the same day
        lag(outlet_code) over (partition by membership_id order by tran_at asc, tran_hour asc) as member_pre_order_branch, ---to detect if this is the bill splited at the same branch

        sum(net_sales) over(partition by tran_day) / count(*) over(partition by tran_day, tran_hour) as daily_net_sales,
        sum(net_sales) over(partition by tran_week) / count(*) over(partition by tran_week, extract(dayofweek from tran_day)) as weekly_net_sales

    from sales
),

joined as (
    select
        sale_logic.*,
        case
            when sale_logic.is_member_order and sale_logic.member_pre_order_branch = sale_logic.outlet_code
                and sale_logic.member_pre_order_day = sale_logic.tran_day and sale_logic.member_order_number_orginal != 1 then 1
            else 0
        end as is_member_order_splited_bill,
        campaigns.voucher_campaign_name as campaign_name,
        campaigns.voucher_id as campaign_voucher_id,
        bis_code.department_code,
        bis_code.function_code,
        bis_code.department_name,
        bis_code.bu_1 as bu,
        bis_code.bu_2,
        bis_code.bu_3,
        bis_code.zone,
        bis_code.owner,
        bis_code.accessed_email,
        bis_code.district,
        bis_code.function_name,
        bis_code.post_code as post_code,
        bis_code.province,
        bis_code.director,
        bis_code.function_manager,
        bis_code.department_manager,
        bis_code.so_at

    from sale_logic
    left join bis_code
        on sale_logic.outlet_code = bis_code.outlet_code
    left join campaigns
        on sale_logic.e_voucher = campaigns.voucher_id
            and datetime(sale_logic.tran_at) between campaigns.started_at and campaigns.ended_at
            and bis_code.bu_1 = campaigns.bu

)

select
    *,
    row_number() over (partition by membership_id, is_member_order_splited_bill order by tran_at asc, tran_hour asc) as member_order_number
from joined
