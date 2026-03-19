with baba as (
select
player_id,
'spełnia warunki' as status
from PROD.DWH.F_TICKET_SELECTION ss
left join (select tournament_id, tournament_name from PROD.DWH.D_TOURNAMENT group by all) bb
    on ss.tournament_id = bb.tournament_id
where business_domain_id = 3 
    and accepted_dt::date >= '2026-03-19'
    and tournament_name in ('UEFA - Europa League', 'UEFA - Conference League')
    and initial_ticket_coefficient >= 2.5
    and stake_total_amt >= 25
group by all
)

select 
ff.*,
baba.status
from PROD.DWH.F_PLAYER_CAMPAIGN ff
left join baba
    on ff.player_id = baba.player_id
where 
campaign_code = '09589ce4-0b8f-408d-a73d-e90e521169ae'
and player_campaign_status = 'opted_in'
order by player_campaign_status_update_dt asc 
