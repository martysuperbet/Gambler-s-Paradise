-- Actives in current year who have not placed SGA bet in the past 3 months 
-- Sportsbook Robert Madzia 

with player as (
select
    distinct 
    player_id,
    status,
    first_name,
    last_name,
    username,
    value_tier_name,
    date_trunc('day',registration_dt) as registration_dt,
    
from PROD.DWH.D_PLAYER pl
where business_market_id = 3
    and business_line_id = 1
    and is_test_account <> 1
),

builder as (
Select distinct 
    player_id

from PROD.DM_SPORT.F_TICKET_SELECTION_SPORT_CRM -- table has only past 3 months
where business_market_id = 3
    and selection_bet_type  in ('SuperBets', 'BetBuilder')
    
),

criteria_met as (
select
    pp.*
from player pp
--left join builder bb
    --on pp.player_id = bb.player_id
where pp.player_id not in (select distinct player_id from builder)
    and pp.status in ('VERIFICATION_PENDING', 'ACTIVE', 'VERIFIED')
),


activity as (
Select 
    PLAYER_ID,              
    MAX(reporting_date) as last_active_day,     
    DATEDIFF('day',  CURRENT_DATE()-1,   MAX(reporting_date) )*-1 as last_activity_was_days_ago 
From PROD.DM_PLAYER.F_PLAYER_PRODUCT_PERFORMANCE_DAILY  
where reporting_date <= CURRENT_DATE()-1
    and business_line_id=1 
    and business_domain_id = 3
group by 1
),

fin as (
select
cc.*,
vv.last_active_day,
case when last_activity_was_days_ago > 31 and last_activity_was_days_ago <= 90 Then 'Churn'
    when last_activity_was_days_ago > 90 and last_activity_was_days_ago <= 364 Then 'Dormant'
    when last_activity_was_days_ago is null then 'No activity'
    when last_activity_was_days_ago <= 30 then 'Active last 30 days'
    else 'not classified' end as Activity,
from criteria_met cc
left join activity vv
    on cc.player_id = vv.player_id
where activity <> 'No activity'
and activity <> 'not classified'
--and value_tier_name <> 'No Bet Last 12 Months'
)

select * from fin
