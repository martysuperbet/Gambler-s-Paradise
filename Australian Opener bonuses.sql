-- first ticket only for Australian Open (within a time period)
-- one of teh AO selections must be LOST
-- only live (INPLAY)
-- min ticket coefficient 2
-- min stake 25 PLn


with first_atp_ticket as (
select
    player_id,
    ticket_code,
    accepted_dt_local as first_ticket
from (
    select
        player_id,
        ticket_code,
        accepted_dt_local,
        row_number() over (partition by player_id order by accepted_dt_local) as rn
    from PROD.DM_SPORT.F_TICKET_SELECTION_SPORT_CRM
    where BUSINESS_DOMAIN_ID = 3
        and tournament_name = 'ATP - Australian Open'
        and accepted_dt_local >= '2026-01-18 00:00:00.01'
)
where rn = 1
),

selection_status as (
select 
    selection_id,
    selection_odd_type
from PROD.DWH.F_TICKET_SELECTION
where BUSINESS_DOMAIN_ID = 3
and tournament_id = '84330'
and selection_odd_type = 'INPLAY'
and ACCEPTED_DT::date >= '2026-01-17'
),

criteria as (
select
    player_id,
    ticket_code,
    selection_id,
    selection_status,
    stake_total_amt,
    initial_ticket_coefficient,
    accepted_dt_local,
    tournament_name
from PROD.DM_SPORT.F_TICKET_SELECTION_SPORT_CRM
where BUSINESS_DOMAIN_ID = 3
and tournament_name = 'ATP - Australian Open'
and accepted_dt_local >= '2026-01-18 00:00:00.01'
and initial_ticket_coefficient >= 2
and selection_status = 'LOST'
and stake_total_amt >= 25
),

bonus_received as (
select
    player_id,
    bonus_name,
    bonus_code,
    bonus_id,
    bonus_rewarded_dt
from PROD.DWH.D_PLAYER_BONUS
where BUSINESS_DOMAIN_ID = 3
and bonus_name = 'CRM_RET_Csbk_25pln_AustralianOpen_LIVE_18.01'
--bonus_code = 'bb540c89-a8a8-41d5-88d5-b59fb8a130f3'
group by all
)

select

cc.player_id,
pp.username,
cc.ticket_code,
    cc.stake_total_amt,
    cc.initial_ticket_coefficient,
    cc.accepted_dt_local,
    cc.tournament_name,

ss.selection_odd_type,
bb.bonus_name

from criteria cc
join first_atp_ticket ff
    on ff.ticket_code = cc.ticket_code
left join selection_status ss
    on cc.selection_id = ss.selection_id
left join bonus_received bb
    on cc.player_id = bb.player_id 
left join (select distinct player_id, username from PROD.DWH.D_PLAYER where BUSINESS_DOMAIN_ID = 3) pp
    on cc.player_id = pp.player_id 
where ss.selection_odd_type is not null
group by all
order by player_id desc
