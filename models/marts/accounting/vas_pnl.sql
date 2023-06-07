with ledgers as (
    select *
        , left( account_id , 3)  as account_id_level_1
    from {{ ref('stg_ipos_accounting__ledgers') }}
    where true
        and is_booked='true'
        and cast (left( account_id , 3) as int64) between 500 and 900 
)

, vas_account as (
    select *
    from {{ ref('stg_ledger_account_vas') }}
)

,amount_incurring as (
    select
        date_trunc(ledgers.tran_at, month) as tran_month,
        vas_account.account_level_1 as pl1,
        vas_account.account_level_2 as pl2,
        vas_account.general_name as pl1_general,
        vas_account.name_level_1 as pl1_name,
        vas_account.name_level_2 as pl2_name,
        ledgers.account_id,
        sum (
            case 
                when ledgers.debit_credit = 'DEB' then ledgers.amount
                when ledgers.debit_credit = 'CRD' then -ledgers.amount
            end 
        )
        as amount    

    from ledgers
    left join vas_account
        on ledgers.account_id_level_1 = vas_account.account_level_1
    where true
        and account_id_contra != '911'
    {{ dbt_utils.group_by(n=7) }}
)
,final as (
    select
        tran_month
        ,cast (pl1 as int64) pl1
        ,pl1_general
        ,pl1_name
        ,sum (amount) amount
        -- ,sum (amount) over ( partition by tran_month, pl1_general ) amount_accumulative

    from amount_incurring
    -- where tran_month ='2022-02-01'
    {{ dbt_utils.group_by(n=4) }}
)

select
        tran_month
        ,pl1
        ,pl1_general
        ,pl1_name
        ,amount

    from final

union all

select
        tran_month
        ,522
        ,'' 
        ,'Gross Sale'
        , sum (amount)

    from final
    where true 
    and pl1 between 511 and 521 
    and pl1!=515
    {{ dbt_utils.group_by(n=4) }}

union all
select
        tran_month
        ,633 
        ,'' 
        ,'Gross profit'
        , sum (amount)

    from final
    where true 
    and pl1 between 511 and 632 
    and pl1!=515
    {{ dbt_utils.group_by(n=4) }}

union all
select
        tran_month
        ,643 
        ,'' 
        ,'Operating profit'
        , sum (amount)

    from final
    where true 
    and pl1 between 511 and 642 
    {{ dbt_utils.group_by(n=4) }}

union all
select
        tran_month
        ,812 
        ,'' 
        ,'Other profit'
        , sum (amount)

    from final
    where true 
    and pl1 between 711 and 811 
    {{ dbt_utils.group_by(n=4) }}

union all
select
        tran_month
        ,813 
        ,'' 
        ,'Net profit before tax'
        , sum (amount)

    from final
    where true 
    and pl1 between 511 and 811 
    {{ dbt_utils.group_by(n=4) }}

union all
select
        tran_month
        ,822 
        ,'' 
        ,'Net profit after tax'
        , sum (amount)

    from final
    where true 
    and pl1 between 511 and 821 
    {{ dbt_utils.group_by(n=4) }}    