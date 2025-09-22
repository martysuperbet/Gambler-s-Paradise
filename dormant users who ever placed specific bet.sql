-- List of dormant users who have ever placed a bet on Polish leagues II, II or IV
-- used for superscore Marcin Bratkowski

with players as (
select
    player_id,
    has_marketing_consent,
from PROD.DWH.D_PLAYER_MARKETING_PROPERTY
where business_domain_id = 3 
    and current_timestamp()  between valid_from_dt and valid_to_dt
    and has_marketing_consent = 1
),

details as (
select
    distinct 
    player_id,
    username,
    email,
    
from PROD.DWH.D_PLAYER pl
where business_market_id = 3
    and business_line_id = 1
    and is_test_account <> 1
),

fin as (
select
    gg.player_id,
    username,
    email,
    count(distinct ticket_code) as tickets
from (
    select
        ff.player_id,
        ff.ticket_code,
    from PROD.DWH.F_TICKET_SELECTION ff
    left join PROD.DWH.D_TOURNAMENT tt
        on ff.tournament_id = tt.tournament_id
    where BUSINESS_DOMAIN_ID = 3
    --and accepted_dt::date >= '2025-01-01'
    and tournament_name like '%Poland - Liga IV%'
    or tournament_name like '%Poland - III Liga%'
    or tournament_name like '%Poland - II Liga%'
    group by all
) gg
left join details
    on gg.player_id = details.player_id
where gg.player_id in (select distinct player_id from players)
group by 1,2,3
having count(distinct ticket_code) >= 1
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
)

select
    cc.*,
    vv.last_active_day,
case when last_activity_was_days_ago > 31 and last_activity_was_days_ago <= 90 Then 'Churn'
    when last_activity_was_days_ago > 90 and last_activity_was_days_ago <= 364 Then 'Dormant'
    when last_activity_was_days_ago is null then 'No activity'
    when last_activity_was_days_ago <= 30 then 'Active last 30 days'
    else 'not classified' end as Activity,
from fin cc
left join activity vv
    on cc.player_id = vv.player_id
where activity = 'Dormant'
