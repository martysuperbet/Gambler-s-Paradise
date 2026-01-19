-- Top 10 days in December for Quintile 5 Medium Value players
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
and date_trunc(month,accepted_dt::date) >= '2025-11-01'
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


-- Player-level metrics for quintile assignment
player_metrics as (
    select
        player_id,
        monthly_official_segmentation,
        count(distinct bet_date) as active_days,
        avg(daily_turnover) as avg_daily_turnover_active_days,
        median(daily_turnover) as median_daily_turnover_active_days,
        avg(daily_tickets) as avg_daily_tickets_active_days,
        sum(daily_turnover) as total_turnover
    from daily_player_activity
    group by 1, 2
),

-- Ticket-level data for median bet size
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
        and date_trunc(month,accepted_dt::date) >= '2025-11-01'
        and stake_total_amt > 0
    group by 1, 2, 3
),


-- Player stake statistics for quintile assignment
player_stake_stats as (
    select
        player_id,
        monthly_official_segmentation,
        median(stake) as median_bet_size,
        avg(stake) as avg_stake
    from ticket_stake
    group by 1, 2
),

-- Assign quintiles
player_with_quintile as (
    select
        pm.player_id,
        pm.monthly_official_segmentation,
        ntile(5) over (order by pss.median_bet_size) as quintile_by_median_bet
    from player_metrics pm
    join player_stake_stats pss
        using (player_id, monthly_official_segmentation)
),

-- Filter for Quintile 5 players only
quintile_5_players as (
    select player_id
    from player_with_quintile
    where quintile_by_median_bet = 5
),

-- Daily activity for Q5 players in December only
december_daily_activity as (
    select
        bet_date,
        player_id,
        daily_tickets as player_daily_tickets,
        daily_turnover as player_daily_turnover,

    from daily_player_activity
        where bet_date::date between '2025-12-01' and '2025-12-31'
    group by all
),

-- Aggregate by day
daily_summary as (
    select
        bet_date,
        count(distinct player_id) as active_players,
        sum(player_daily_turnover) as total_turnover,
        sum(player_daily_turnover)/count(distinct player_id) as avg_to_mine,
        avg(player_daily_turnover) as avg_turnover_per_player,
        sum(player_daily_tickets) as total_tickets,
        avg(player_daily_tickets) as avg_tickets_per_player
    from december_daily_activity
    group by bet_date
)
/*
-- Top 10 days by number of active players
select
    bet_date,
    dayname(bet_date) as day_of_week,
    active_players,
    total_turnover,
    avg_turnover_per_player,
    total_tickets,
    avg_tickets_per_player
from daily_summary
order by active_players desc
limit 10;
*/
-- Uncomment below to get Top 10 days by highest turnover instead:

select
    bet_date,
    dayname(bet_date) as day_of_week,
    active_players,
    total_turnover,
    avg_turnover_per_player,
    avg_to_mine,
    total_tickets,
    avg_tickets_per_player
from daily_summary
order by total_turnover desc
limit 10;
