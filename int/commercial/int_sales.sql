{{
    config(
        materialized='table'
    )
}}

with sales as (
    select * from {{ ref('stg_ipos_sales') }}
),

delivery_information as (
    select * from {{ ref('stg_delivery_information') }}
),

member_type_discount as (
    select * from {{ ref('int_member_type_discount_joined') }}
),

sale_details as (
    select
        unique_sale_id,
        sum(gross_sales) as gross_sales,
        min(time_in) as time_in,
        max(time_out) as time_out,
        sum(item_discount) as item_discount

    from {{ ref('int_sale_detail_outlets') }}
    group by 1
),

joined_sales as (
    select
        sales.*,
        sale_details.gross_sales,
        sale_details.time_in,
        sale_details.time_out,
        sale_details.item_discount,
        sale_details.gross_sales * sales.vat as amount_vat,
        sale_details.gross_sales * sales.service_charge_percent as service_charge,
        sale_details.gross_sales * sales.shift_charge as delivery_fee,
        coalesce(member_type_discount.member_discount_pecent,0) as member_discount_pecent,
        coalesce(sale_details.gross_sales * member_type_discount.member_discount_pecent,0) as membership_discount,
        delivery_information.party_name as delivery_party_name,
        member_type_discount.membership_type_name

    from sales
    left join sale_details
        on sales.unique_id = sale_details.unique_sale_id
    left join delivery_information
        on sales.delivery_party_id = delivery_information.party_id
    left join member_type_discount
        on sales.membership_type_id = member_type_discount.membership_type_id
            and sales.tran_at between member_type_discount.started_at and member_type_discount.ended_at
),


final as (
    select
        unique_id,
        outlet_code,
        delivery_party_name,
        membership_type_id,
        membership_type_name,
        membership_id,
        membership_id is not null as is_member_order,
        0 as cash_voucher,
        e_voucher,
        tran_at,
        total_customers,
        male,
        female,
        member_discount_pecent,
        round(coupon_amount / 1000,0) as promotion,
        round(service_charge / 1000,0) as service_charge,
        round((item_discount + membership_discount) / 1000,0) as discount,
        round(delivery_fee / 1000,0) as delivery_fee,
        round(gross_sales / 1000,0) as gross_sales,
        round(amount_vat / 1000,0) as amount_vat,
        time_in,
        time_out,
        round(
            (
                gross_sales
                -- amount_vat
                - coupon_amount
                - item_discount
                - membership_discount
                + service_charge
                + delivery_fee
            ) / 1000,0
        )
        as net_sales,

        date_trunc(tran_at,month) as tran_month,
        date_trunc(tran_at,week(monday)) as tran_week,
        date_trunc(tran_at,day) as tran_day,
        extract(hour from time_in) as tran_hour,
        1 as tc,
        --case when total_customers = 0 then net_sales else net_sales/total_customers end as ta_customers,
        case
            when time_in < time_out then time_diff(time_out,time_in, minute)
            else timestamp_diff(
                timestamp_add(timestamp(concat(date(tran_at), ' ',time_out)), interval 1 day),
                timestamp(concat(date(tran_at), ' ',time_in)),
                minute
                )
        end as duration

    from joined_sales
)

select * from final
