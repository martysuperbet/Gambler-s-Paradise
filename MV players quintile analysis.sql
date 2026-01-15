--MEDIAN VALUE PLAYERS: VALUE DISTRIBUTION BY QUINTILES 
--Final query quintile by median, optional by agverage 

-- Medium Value segmentation analysis by quintiles
with segment as (
    select distinct 
        player_id,
        ACTUAL_OFFICIAL_SEGMENTATION as monthly_official_segmentation
    from PROD.DWH.D_PLAYER
    left join PROD.BI.VW_PLAYER_MONTHLY_SEGMENTATION_ALL sgm
        using (player_id)
    where is_valid = 1 
        and business_line_id = 1 
        and business_domain_id = 3
        and is_test_account <> 1
        and segmentation_month = '2026-01-01'
        and ACTUAL_OFFICIAL_SEGMENTATION in ('Medium Value')
),

ticket_stake_1 as (
select
ticket_code,
ss.Player_id,
date_trunc(day,accepted_dt) as accepted_dt_local,
monthly_official_segmentation,
max(stake_total_amt) as stake_total_amt_eur,
from PROD.DWH.F_TICKET ff
join segment ss 
on ss.player_id = ff.player_id
where business_domain_id = 3 
and accepted_dt >= '2025-11-01'
and lower(ticket_status) <> 'cancelled'
and stake_total_amt > 0
group by all
),

-- Daily player activity with stakes
daily_player_activity as (
    select
        ff.player_id,
        monthly_official_segmentation,
        date(accepted_dt_local) as bet_date,
        count(distinct ticket_code) as daily_tickets,
        sum(stake_total_amt_eur) as daily_turnover
    from ticket_stake_1 ff
    group by 1, 2, 3
),

-- Player-level daily metrics (one row per player per day)
player_metrics as (
    select
        player_id,
        monthly_official_segmentation,
        count(distinct bet_date) as active_days,
        avg(daily_turnover) as avg_daily_turnover_active_days,
        median(daily_turnover) as median_daily_turnover_active_days,
        avg(daily_tickets) as avg_daily_tickets_active_days,
        sum(daily_turnover) as total_turnover  -- Total across all days for this player
    from daily_player_activity
    group by 1, 2
),

-- Ticket-level data (one row per ticket)
ticket_stake as (
    select
        ticket_code,
        ff.player_id,
        ss.monthly_official_segmentation,
        max(stake_total_amt) as stake
    from PROD.DWH.F_TICKET ff
    join segment ss 
        on ss.player_id = ff.player_id
    where business_domain_id = 3 
        and accepted_dt >= '2025-11-01'
        and stake_total_amt > 0
    group by 1, 2, 3
),

-- Player stake statistics for quintile assignment (one row per player)
player_stake_stats as (
    select
        player_id,
        monthly_official_segmentation,
        median(stake) as median_bet_size,
        avg(stake) as avg_stake
    from ticket_stake
    group by 1, 2
),

-- Combine metrics and assign quintiles (one row per player)
player_combined as (
    select
        pm.player_id,
        pm.monthly_official_segmentation,
        pm.active_days,
        pm.avg_daily_turnover_active_days,
        pm.median_daily_turnover_active_days,
        pm.avg_daily_tickets_active_days,
        pm.total_turnover,
        pss.median_bet_size,
        pss.avg_stake,
        ntile(5) over (order by pss.median_bet_size) as quintile_by_median_bet,
        ntile(5) over (order by pm.avg_daily_turnover_active_days) as quintile_by_avg_daily_turnover
    from player_metrics pm
    join player_stake_stats pss
        using (player_id, monthly_official_segmentation)
),

-- Add quintile to tickets (one row per ticket with its player's quintile)
tickets_with_quintiles as (
    select
        ts.ticket_code,
        ts.player_id,
        ts.stake,
        pc.quintile_by_median_bet,
        pc.quintile_by_avg_daily_turnover
    from ticket_stake ts
    join player_combined pc
        on ts.player_id = pc.player_id
),

-- Stake metrics by quintile (aggregated from tickets)
stake_by_quintile as (
    select
        quintile_by_median_bet,
        median(stake) as quintile_median_stake,
        avg(stake) as quintile_avg_stake,
        count(distinct ticket_code) as total_tickets,
        count(distinct player_id) as players_check  -- Should match players count
    from tickets_with_quintiles
    group by quintile_by_median_bet
),

-- Player metrics by quintile (aggregated from players, not tickets)
player_by_quintile as (
    select
        quintile_by_median_bet,
        count(distinct player_id) as players,
        avg(avg_daily_turnover_active_days) as avg_daily_turnover_active_days,
        avg(median_daily_turnover_active_days) as avg_median_daily_turnover_active_days,
        avg(avg_daily_tickets_active_days) as avg_daily_tickets_active_days,
        avg(active_days) as avg_active_days,
        sum(total_turnover) as quintile_total_turnover  -- Each player counted once
    from player_combined
    group by quintile_by_median_bet
),
/*
-- Final analysis by quintiles
select
    pbq.quintile_by_median_bet,
    pbq.players,
    sbq.total_tickets,
    
    -- Stake metrics - calculated from ALL tickets in the quintile
    sbq.quintile_median_stake,
    sbq.quintile_avg_stake,
    
    -- Player-level averages (averaged across players in quintile)
    pbq.avg_daily_turnover_active_days,
    pbq.avg_median_daily_turnover_active_days,
    pbq.avg_daily_tickets_active_days,
    pbq.avg_active_days,
    
    -- Turnover contribution (summed once per player)
    pbq.quintile_total_turnover,
    pbq.quintile_total_turnover / sum(pbq.quintile_total_turnover) over () as pct_of_total_turnover,
    sum(pbq.quintile_total_turnover) over (order by pbq.quintile_by_median_bet) / 
        sum(pbq.quintile_total_turnover) over () as cumulative_pct_turnover

from player_by_quintile pbq
join stake_by_quintile sbq 
    on pbq.quintile_by_median_bet = sbq.quintile_by_median_bet
order by pbq.quintile_by_median_bet;
*/

-- Optional: Analysis by avg daily turnover quintiles

stake_by_quintile_adt as (
    select
        quintile_by_avg_daily_turnover,
        median(stake) as quintile_median_stake,
        avg(stake) as quintile_avg_stake,
        count(distinct ticket_code) as total_tickets,
        count(distinct player_id) as players_check
    from tickets_with_quintiles
    group by quintile_by_avg_daily_turnover
),

player_by_quintile_adt as (
    select
        quintile_by_avg_daily_turnover,
        count(distinct player_id) as players,
        avg(avg_daily_turnover_active_days) as avg_daily_turnover_active_days,
        avg(median_daily_turnover_active_days) as avg_median_daily_turnover_active_days,
        avg(avg_daily_tickets_active_days) as avg_daily_tickets_active_days,
        avg(active_days) as avg_active_days,
        sum(total_turnover) as quintile_total_turnover
    from player_combined
    group by quintile_by_avg_daily_turnover
)

select
    pbq.quintile_by_avg_daily_turnover,
    pbq.players,
    sbq.total_tickets,
    sbq.quintile_median_stake,
    sbq.quintile_avg_stake,
    pbq.avg_daily_turnover_active_days,
    pbq.avg_median_daily_turnover_active_days,
    pbq.avg_daily_tickets_active_days,
    pbq.avg_active_days,
    pbq.quintile_total_turnover,
    pbq.quintile_total_turnover / sum(pbq.quintile_total_turnover) over () as pct_of_total_turnover,
    sum(pbq.quintile_total_turnover) over (order by pbq.quintile_by_avg_daily_turnover) / 
        sum(pbq.quintile_total_turnover) over () as cumulative_pct_turnover
from player_by_quintile_adt pbq
join stake_by_quintile_adt sbq 
    on pbq.quintile_by_avg_daily_turnover = sbq.quintile_by_avg_daily_turnover
order by pbq.quintile_by_avg_daily_turnover;
