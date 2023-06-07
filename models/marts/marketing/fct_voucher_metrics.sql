with vouchers as (
    select * from {{ ref('stg_ipos_crm__member_vouchers') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

voucher_order_grouped as (
    select
        orders.tran_at as created_at,
        orders.membership_id,
        coalesce(orders.campaign_name,vouchers.voucher_campaign_name) as campaign_name,
        case when vouchers.voucher_campaign_name is null then concat(orders.e_voucher,date(orders.tran_at)) else orders.e_voucher end as voucher_code,
        vouchers.voucher_code as v1

    from orders
    left join vouchers
        on orders.e_voucher = vouchers.voucher_code
            and orders.membership_id = vouchers.membership_id
    where orders.e_voucher is not null

),

issued_unioned as (
    select
        created_at,
        membership_id,
        voucher_code,
        campaign_name,
        1 as number_of_vouchers,
        'Issued' as event_type,
        'order_soure' as sources

    from voucher_order_grouped
    where v1 is null

    union all

    select
        created_at as created_at,
        membership_id,
        voucher_code,
        voucher_campaign_name as campaign_name,
        1 as number_of_vouchers,
        'Issued' as event_type,
        'member_voucher' as sources

    from vouchers

    union all

    select
        created_at,
        membership_id,
        voucher_code,
        campaign_name,
        1 as number_of_vouchers,
        'Used' as event_type,
        'order_soure' as sources
    from voucher_order_grouped

),

final as (
    select
        *,
        {{ dbt_utils.surrogate_key(['created_at','membership_id','voucher_code','event_type','sources']) }} as unique_id

    from issued_unioned
)


select * from final
