select 
    mm.creator,
    player_id
from PROD.BI.MACZKA_LIST mm --uploaded excel (one-off)
left join 
    ( select 
        date_trunc(month, CONVERT_TIMEZONE('UTC','Europe/Warsaw',reporting_date)) as month,
        mm.player_id,
        social_username,
        sum(copy_tickets_count) copie,
        sum(copy_users_count) as users,
        sum(eligible_inspired_turnover) as eligible_inspired_turnover
    from PROD.REPORTING_PRODUCT.VW_SOCIAL_CONTENT_CREATOR_METRICS mm
    left join (select Player_id, social_username
    from PROD.DM_SOCIAL.D_PLAYER_SOCIAL where business_domain_id = 3
    group by all) pp
        on mm.player_id = pp.player_id
    where business_domain_id in (3)
        and date_trunc(month, CONVERT_TIMEZONE('UTC','Europe/Warsaw',reporting_date)) = '2025-10-01'
    group by all
    having sum(copy_tickets_count) < 35 
        or sum(copy_users_count) < 10
    ) pp
on mm.creator = pp.social_username
group by all
