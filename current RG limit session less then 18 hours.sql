-- current RG limit session less then 18 hours
select distinct 
    dd.player_id,
    username,
    value_tier_name,
    limit_type,
    limit_interval,
    measurement_unit,
    limit_amount,
    limit_status,

from PROD.DWH.D_PLAYER_LIMIT dd
left join (select
distinct
    player_id,
    username,
    value_tier_name
from PROD.DWH.D_PLAYER
where business_domain_id = 3 ) pp
    on dd.player_id = pp.player_id
where business_domain_id = 3 
    and limit_type = 'session'
    and limit_interval = 'daily'
    and limit_status = 'active'
    and measurement_unit = 'hour'
    and limit_amount < 18
    and current_date() between valid_from_dt and valid_to_dt
order by 1 desc