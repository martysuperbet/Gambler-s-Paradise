/*
1.Users who have published at least 5 coupons on Supersocial in the last month
2.Users, excluding the first group, who have copied at least 5 coupons on Supersocial
3.Other Superbet users with consent (excluding the first 2 groups)
*/
--poc Marcin Bratkowski

with player as (
select
player_id
from PROD.DWH.D_PLAYER_MARKETING_PROPERTY
where BUSINESS_DOMAIN_ID = 3
and current_timestamp() between valid_from_dt and valid_to_dt
and has_marketing_consent = 1
group by all
),

--run only for point 1
zero_activity as (
select distinct
    ss.player_id,
    username,
    value_tier_name_official
    
from PROD.DM_SOCIAL.F_PLAYER_ENGAGEMENT_DAILY ss
left join (select 
            player_id,
            username
            from PROD.DWH.D_PLAYER
            where business_domain_id = 3
            and is_test_account = 0) dd
on ss.player_id = dd.player_id
left join (select distinct
            player_id,
            value_tier_name_official
        from DM_PLAYER.F_PLAYER_VALUE_TIER_MONTHLY 
            where date_trunc('month', REPORTING_DATE_FROM) = '2025-10-01'
            AND BUSINESS_DOMAIN_ID = 3) gg
on ss.player_id = gg.player_id
where date_trunc(month,reporting_date) = '2025-09-01'
    and business_domain_id = 3
    and username is not null
    and ss.player_id in (select distinct player_id from player)
group by all
having sum(ticket_shares_cnt) >= 5
--and sum(ticket_copy_ticket_cnt) = 0
),

--run only for point 2
nono as (
select distinct
    ss.player_id,
    username,
    value_tier_name_official
    
from PROD.DM_SOCIAL.F_PLAYER_ENGAGEMENT_DAILY ss
left join (select 
            player_id,
            username
            from PROD.DWH.D_PLAYER
            where business_domain_id = 3
            and is_test_account = 0) dd
on ss.player_id = dd.player_id
left join (select distinct
            player_id,
            value_tier_name_official
        from DM_PLAYER.F_PLAYER_VALUE_TIER_MONTHLY 
            where date_trunc('month', REPORTING_DATE_FROM) = '2025-10-01'
            AND BUSINESS_DOMAIN_ID = 3) gg
on ss.player_id = gg.player_id
where date_trunc(month,reporting_date) = '2025-09-01'
    and business_domain_id = 3
    and username is not null
    and ss.player_id in (select distinct player_id from player)
    and ss.player_id not in (select distinct player_id from zero_activity)
group by all
having --sum(ticket_shares_cnt) >= 5
sum(ticket_copy_ticket_cnt) >= 5
)

--run for point 3
select distinct
dd.player_id,
username
from player dd
left join (select distinct player_id, status, USERNAME, is_test_account
from PROD.DWH.D_PLAYER
where business_domain_id = 3) ss
    on dd.player_id = ss.player_id

where dd.player_id not in (select distinct player_id from zero_activity)
    and dd.player_id not in (select distinct player_id from nono)
    and is_test_account <> 1
    and status in ('ACTIVE', 'REGISTERED','VERIFIED','VERIFICATION_PENDING')
