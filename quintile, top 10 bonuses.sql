-- Top 10 most used bonuses for Quintile 5 Medium Value players in December
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
    where quintile_by_median_bet = 4
    
),

-- Bonuses redeemed by Q5 players in December
q5_december_bonuses as (
    select
        bb.player_id,
        bb.bonus_name,
        bc.BONUS_CATEGORY,
        cc.campaign_name,
        cc.campaign_period,
        bb.campaign_id,
        bb.bonus_rewarded_dt,
        bb.bonus_redeemed_amount
    from PROD.DWH.D_PLAYER_BONUS bb
    join quintile_5_players q5
        on bb.player_id = q5.player_id
    join PROD.DWH.D_CAMPAIGN cc
        on bb.campaign_id = cc.campaign_id
    LEFT JOIN  PROD.BI.VW_ALL_DWH_D_BONUS_CATEGORY bc 
        ON bb.BONUS_NAME = bc.BONUS_NAME
        AND bb.BUSINESS_DOMAIN_id =bc.BUSINESS_DOMAIN_id
    where bb.business_domain_id = 3 
        and date_trunc(month, bb.bonus_rewarded_dt)::date = '2025-12-01'
)

-- Top 10 most frequently used bonuses
select
    bonus_name,
    BONUS_CATEGORY,
    campaign_name,
    campaign_period,
    count(distinct player_id) as unique_players,
    count(*) as total_redemptions,
    sum(bonus_redeemed_amount) as total_bonus_value,
    avg(bonus_redeemed_amount) as avg_bonus_value,
    median(bonus_redeemed_amount) as median_bonus_value,
    min(bonus_rewarded_dt) as first_redemption,
    max(bonus_rewarded_dt) as last_redemption,
    -- Show most common campaign name for this bonus
    mode(campaign_name) as most_common_campaign
from q5_december_bonuses
group by all
order by total_redemptions desc
limit 30
