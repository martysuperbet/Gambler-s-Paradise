-- players who were logged in during specific hours
-- between 2025-08-28 20:40:00.000 and 2025-08-28 22:25:00.000

with cet as (
select distinct
    PS.PLAYER_CODE,
    PS.player_id,
    dd.username,
    dd.email,
    VALUE_TIER_NAME_OFFICIAL,
    ngp_lt,
    --CONVERT_TIMEZONE('UTC', 'CET',PS.SESSION_START_DT)
from DWH.F_PLAYER_SESSION ps
left join (select 
    player_id,
    username,
    email
    from PROD.DWH.D_PLAYER dd
    where business_domain_id = 3
    and is_test_account <> 1) dd
on PS.player_id = dd.player_id
LEFT JOIN (select distinct player_id,
            VALUE_TIER_NAME_OFFICIAL
    from DM_PLAYER.F_PLAYER_VALUE_TIER_MONTHLY
    where date_trunc('month', REPORTING_DATE_FROM) = '2025-08-01'
    and business_domain_id = 3 ) rr
    on ps.player_id = rr.player_id
left join (select
    player_id,
    sum(ngp) as ngp_lt
    
from PROD.DM_PLAYER.F_PLAYER_PRODUCT_PERFORMANCE_DAILY sp 
where sp.business_domain_id = 3
    and bet_total <> 0
group by 1) sp
on ps.player_id = sp.player_id
where BUSINESS_DOMAIN_ID = 3
and CONVERT_TIMEZONE('UTC', 'Europe/Warsaw',PS.SESSION_START_DT) >= '2025-08-28 20:40:00.000'
and CONVERT_TIMEZONE('UTC', 'Europe/Warsaw',PS.SESSION_START_DT) <= '2025-08-28 22:25:00.000'
)

select distinct
    PS.PLAYER_CODE,
    PS.player_id,
    dd.username,
    dd.email,
    VALUE_TIER_NAME_OFFICIAL,
    ngp_lt
    --CONVERT_TIMEZONE('UTC', 'CET',PS.SESSION_START_DT)
from DWH.F_PLAYER_SESSION ps
left join (select 
    player_id,
    username,
    email
    from PROD.DWH.D_PLAYER dd
    where business_domain_id = 3
    and is_test_account <> 1) dd
on PS.player_id = dd.player_id
LEFT JOIN (select distinct player_id,
            VALUE_TIER_NAME_OFFICIAL
    from DM_PLAYER.F_PLAYER_VALUE_TIER_MONTHLY
    where date_trunc('month', REPORTING_DATE_FROM) = '2025-08-01'
    and business_domain_id = 3 ) rr
    on ps.player_id = rr.player_id
left join (select
    player_id,
    sum(ngp) as ngp_lt
    
from PROD.DM_PLAYER.F_PLAYER_PRODUCT_PERFORMANCE_DAILY sp 
where sp.business_domain_id = 3
    and bet_total <> 0
group by 1) sp
on ps.player_id = sp.player_id
where BUSINESS_DOMAIN_ID = 3
and (
        -- session starts in range
        (CONVERT_TIMEZONE('UTC','Europe/Warsaw', ps.SESSION_START_DT) between '2025-08-28 20:40:00.000' and '2025-08-28 22:25:00.000')
        -- session ends in range
     or (CONVERT_TIMEZONE('UTC','Europe/Warsaw', ps.SESSION_END_DT) between '2025-08-28 20:40:00.000' and '2025-08-28 22:25:00.000')
        -- session spans across the entire window
     or (CONVERT_TIMEZONE('UTC','Europe/Warsaw', ps.SESSION_START_DT) < '2025-08-28 20:40:00.000'
         and CONVERT_TIMEZONE('UTC','Europe/Warsaw', ps.SESSION_END_DT) > '2025-08-28 22:25:00.000')
      )
and ps.player_id not in (select distinct player_id from cet)