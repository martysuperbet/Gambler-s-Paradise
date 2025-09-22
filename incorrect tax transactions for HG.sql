-- find incorrect tax transactions for Highlight Games 

with fin as (
select 
    ffrb.player_code,
    dp.username,
    dp.first_name,
    dp.last_name,
    ffrb.TRANSACTION_DT_LOCAL,
    ffrb.wallet_transaction_id,
    ffrb.wallet_transaction_code,
    coalesce(TICKET_CODE, split_part(GAME_CYCLE_CODE, '-'::varchar(1), CASE WHEN (GAME_CYCLE_CODE LIKE 'HG%'::varchar(3)) THEN 2 ELSE 4 END)) AS TicketID,
    GAME_CYCLE_CODE,
    spin_id,
    spin_code,
    adx.game_provider_code, 
    ffrb.product_type_code,
    ffrb.Transaction_type as transactiontype,
    abs(ffrb.real_money_amount) as game_real,
    abs(ffrb.bonus_money_amount) as game_bonus,
    abs(ffrb.tax_money_amount) as game_tax,
    abs(ffrb.real_money_amount) + abs(ffrb.bonus_money_amount) as stake,
    abs(ffrb.tax_money_amount)/nullifzero(abs(ffrb.real_money_amount)) as diff_real,
    abs(ffrb.tax_money_amount)/nullifzero(abs(ffrb.bonus_money_amount)) as diff_bonus

from PROD.DWH.F_WALLET_TRANSACTION ffrb 
right join (
        select distinct
            game_code,
            game_provider_code,
            game_subprovider_code,
            valid_from_dt,
            valid_to_dt
        
        from PROD.DWH.D_GAME_HISTORY
        where business_domain_id = 3
            and business_line_id = 1
        ) as adx 
on ffrb.game_code = adx.game_code
and ffrb.TRANSACTION_DT_LOCAL between adx.valid_from_dt and adx.valid_to_dt
left join PROD.DWH.D_PLAYER dp
    on dp.player_id = ffrb.player_id
where 1=1
    and lower(ffrb.Transaction_Type) like '%bet%'
    and ffrb.TRANSACTION_DT_LOCAL::date >= '2025-07-17'
    and ffrb.BUSINESS_LINE_ID = 1
    and ffrb.BUSINESS_DOMAIN_ID = 3
    and dp.is_test_account = 0
    and game_provider_code = 'HIGHLIGHT GAMES'

    --limit 100
    --and ffrb.real_money_amount <> 0
    --and ffrb.bonus_money_amount <> 0
    --and abs(ffrb.tax_money_amount) = 0
),

mark as (
select 
    fin.*
from fin 
where round(DIFF_real,2) not in (0.12, 0.13)
    or DIFF_real is null
    and round(DIFF_bonus,2) not in (0.12, 0.13)
order by TRANSACTION_DT_LOCAL desc
),

payouts as (
select 
    ffrb.player_code,
    ffrb.TRANSACTION_DT_LOCAL,
    ffrb.wallet_transaction_id,
    GAME_CYCLE_CODE,
    ffrb.product_type_code,
    ffrb.Transaction_type as transactiontype,
    abs(ffrb.real_money_amount) as payout

from PROD.DWH.F_WALLET_TRANSACTION ffrb 
left join PROD.DWH.D_PLAYER dp
    on dp.player_id = ffrb.player_id
where 1=1
    and lower(ffrb.Transaction_Type) = 'game win'
    and ffrb.TRANSACTION_DT_LOCAL::date >= '2025-07-17'
    and ffrb.TRANSACTION_DT_LOCAL::date <= '2025-07-22'
    and ffrb.BUSINESS_LINE_ID = 1
    and ffrb.BUSINESS_DOMAIN_ID = 3
group by all
)

select 
    mm.*,
    pp.payout
from mark mm
left join payouts pp
on mm.GAME_CYCLE_CODE = pp.GAME_CYCLE_CODE
and mm.player_code = pp.player_code
order by game_real desc 

