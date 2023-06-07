with sources as (
    select * from {{ source('ipos_sale','sale') }}
),

renamed as (
    select
        --id
        {{ dbt_utils.surrogate_key(['pr_key','workstation_name']) }} as unique_id,
        cast(pr_key as string) as sale_id,
        cast(extra_id_2 as string) as delivery_party_id,

        user_id,
        cast(customer_id as string) as customer_id,

        --store
        workstation_name as outlet_code,
        area_id,
        currency_type_id,
        cast(session_id as string) as session_id,
        cast(workstation_id as string) as workstation_id,

        dinner_table_id,
        cast(shift_id as string) as shift_id,

        --timestamp
        timestamp(tran_date) as tran_at,
        timestamp(date_last) as date_last,

        --time extract
        hour_last,
        minute_last,

        --status
        payment_status,
        status,

        --demographic
        number_people as total_customers,
        number_female as female,
        number_male as male,

        --metric
        get_amount,
        deposit_amount,
        return_amount,

        service_charge as service_charge_percent,
        shift_charge,

        coupon_amount,
        coupon_count,

        discount_extra,
        --amount_discount_extra2,

        print_count,

        --vat
        vat,
        vat_tax_code,
        vat_tran_no,
        vat_customer_address,
        vat_payment_method,
        vat_customer_name,
        vat_company_name,
        vat_content,
        vat_sign,

        --code
        card_info_code,

        --pr key
        cast(pr_key_bookings as string) as pr_key_bookings,
        pr_key_ht_folio,

        --tran no
        tran_id,
        tran_no_temp,
        is_temp = 1 as is_temp,
        tran_no,

        --addt info
        amount_point,
        exchange_rate,
        foodbook_order_data,
        address_delivery,
        sale_sign,
        note,

        --source
        case when room_id = '' then string(null) else upper(room_id) end as e_voucher,

        --membership
        case when cast(membership_id as string) = '' then null else membership_id end as membership_id,
        case when cast(membership_type_id as string) = '' then null else membership_type_id end as membership_type_id,
        case when cast(membership_voucher as string) = '' then null else membership_voucher end as membership_voucher,
        case when cast(membership_birthday as string) = '' then null else membership_birthday end as membership_birthday,

        --final metric
        get_amount as amount_org

    from sources
    where lower(workstation_name) not like '%test%'

)

select * from renamed

--extra_id_2	
--pr_key_sale_old
--pr_key_sale_new		
