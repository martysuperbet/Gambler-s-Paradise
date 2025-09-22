-- most recent wallet balance of VIP users
-- PROD.DWH.D_PLAYER_EOD_BALANCE can be used for yesterday's data but it displayes only players with activity for each date

with players as (
select
    player_id,
    username,
    email,
    status
from PROD.DWH.D_PLAYER
where value_tier_name = 'VIP'
    and business_domain_id =  3
    and is_test_account <> 1
    and status <> 'PLAYER_CLOSED'
    and status <> 'SUSPENDED'
group by all
),

fin as (
SELECT player_id,
       REAL_MONEY_BALANCE_AFTER as REAL_MONEY_BALANCE,
       BONUS_MONEY_BALANCE_AFTER as BONUS_MONEY_BALANCE,
       TRANSACTION_DT_LOCAL
FROM (
    SELECT player_id,
       REAL_MONEY_BALANCE_AFTER,
       BONUS_MONEY_BALANCE_AFTER,
       TRANSACTION_DT_LOCAL,
           ROW_NUMBER() OVER (
               PARTITION BY player_id 
               ORDER BY TRANSACTION_DT_LOCAL DESC
           ) AS rn
    FROM PROD.DWH.F_WALLET_TRANSACTION t
    WHERE business_domain_id = 3
    AND PLAYER_ID IN (SELECT DISTINCT PLAYER_ID FROM players)
) x
WHERE rn = 1
)

select 
    fin.*,
    username,
    email,
from fin
left join players
    on fin.player_id = players.player_id
order by REAL_MONEY_BALANCE desc 
