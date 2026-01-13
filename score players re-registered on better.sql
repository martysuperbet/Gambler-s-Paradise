with base as (
select
email,
count(distinct player_id) as ct
from PROD.DWH.D_PLAYER
where business_domain_id = 3
and business_line_id = 1
and is_test_account <> 1
group by all
having count(distinct player_id) > 1
order by 2 desc
),

mix as (
select distinct 
player_id,

from PROD.DWH.D_PLAYER
where business_domain_id = 3
and email in (select distinct email from base)
and source_system_code = 'BETLER'
and registration_dt::date >= '2025-09-01'

)


select
sum(bet_total) as tor,
min(reporting_date)
from PROD.DM_PLAYER.F_PLAYER_PRODUCT_PERFORMANCE_DAILY
where business_domain_id = 3
and player_id in (select distinct player_id from mix)
