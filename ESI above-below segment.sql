-- ESI Calculation with Player Distribution Analysis
-- 1. ESI per player tier (segment-level)
-- 2. Count of players above/below their segment's ESI for each value type

with value_mapping as (
    select
        date_value,
        value
    from PROD.DWH.D_DATE dd
    left join PROD.BI.POLAND_JAN_VALUE_DAYS vv
        on dd.day_in_month = vv.day
    where month_start_date = '2026-01-01'
),

segment as (
    select distinct 
        player_id,
        ACTUAL_OFFICIAL_SEGMENTATION as player_value_tier
    from PROD.DWH.D_PLAYER
    left join PROD.BI.VW_PLAYER_MONTHLY_SEGMENTATION_ALL sgm
        using (player_id)
    where is_valid = 1 
        and business_line_id = 1 
        and business_domain_id = 3
        and is_test_account <> 1
        and segmentation_month = '2026-02-01'
),

ticket_stake as (
    select
        ticket_code,
        ss.player_id,
        ss.player_value_tier,
        date_trunc(day, accepted_dt) as accepted_dt_local,
        max(stake_total_amt) as stake
    from PROD.DWH.F_TICKET ff
    join segment ss 
        on ss.player_id = ff.player_id
    where business_domain_id = 3 
        and accepted_dt >= '2026-01-01'
        and accepted_dt < '2026-02-01'
        and lower(ticket_status) <> 'cancelled'
        and stake_total_amt > 0
    group by ticket_code, ss.player_id, ss.player_value_tier, date_trunc(day, accepted_dt)
),

-- Calculate SEGMENT-LEVEL ESI (overall for each tier)
segment_esi as (
    select
        ts.player_value_tier,
        sum(case when vm.value = 'high' then ts.stake else 0 end) as stake_high_value_days,
        sum(case when vm.value = 'medium' then ts.stake else 0 end) as stake_mid_value_days,
        sum(case when vm.value = 'low' then ts.stake else 0 end) as stake_low_value_days,
        sum(ts.stake) as total_stake,
        round(sum(case when vm.value = 'high' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as segment_esi_high,
        round(sum(case when vm.value = 'medium' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as segment_esi_mid,
        round(sum(case when vm.value = 'low' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as segment_esi_low
    from ticket_stake ts
    left join value_mapping vm
        on ts.accepted_dt_local = vm.date_value
    group by ts.player_value_tier
),

-- Calculate PLAYER-LEVEL ESI
player_esi as (
    select
        ts.player_id,
        ts.player_value_tier,
        sum(ts.stake) as total_stake,
        round(sum(case when vm.value = 'high' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as player_esi_high,
        round(sum(case when vm.value = 'medium' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as player_esi_mid,
        round(sum(case when vm.value = 'low' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as player_esi_low
    from ticket_stake ts
    left join value_mapping vm
        on ts.accepted_dt_local = vm.date_value
    group by ts.player_id, ts.player_value_tier
),

-- Join players with their segment ESI
player_vs_segment as (
    select
        p.player_id,
        p.player_value_tier,
        p.total_stake,
        p.player_esi_high,
        p.player_esi_mid,
        p.player_esi_low,
        s.segment_esi_high,
        s.segment_esi_mid,
        s.segment_esi_low
    from player_esi p
    join segment_esi s
        on p.player_value_tier = s.player_value_tier
)

-- Final summary with player counts compared to SEGMENT ESI
select
    player_value_tier,
    count(distinct player_id) as total_players,
    sum(total_stake) as total_stake_eur,
    
    -- Segment-level ESI (from all tickets in segment)
    max(segment_esi_high) as segment_esi_high,
    max(segment_esi_mid) as segment_esi_mid,
    max(segment_esi_low) as segment_esi_low,
    
    -- HIGH VALUE ESI distribution vs segment ESI
    count(distinct case when player_esi_high >= segment_esi_high then player_id end) as players_esi_high_above_segment,
    count(distinct case when player_esi_high < segment_esi_high then player_id end) as players_esi_high_below_segment,
    round(count(distinct case when player_esi_high >= segment_esi_high then player_id end) * 100.0 / count(distinct player_id), 2) as pct_esi_high_above_segment,
    
    -- MID VALUE ESI distribution vs segment ESI
    count(distinct case when player_esi_mid >= segment_esi_mid then player_id end) as players_esi_mid_above_segment,
    count(distinct case when player_esi_mid < segment_esi_mid then player_id end) as players_esi_mid_below_segment,
    round(count(distinct case when player_esi_mid >= segment_esi_mid then player_id end) * 100.0 / count(distinct player_id), 2) as pct_esi_mid_above_segment,
    
    -- LOW VALUE ESI distribution vs segment ESI
    count(distinct case when player_esi_low >= segment_esi_low then player_id end) as players_esi_low_above_segment,
    count(distinct case when player_esi_low < segment_esi_low then player_id end) as players_esi_low_below_segment,
    round(count(distinct case when player_esi_low >= segment_esi_low then player_id end) * 100.0 / count(distinct player_id), 2) as pct_esi_low_above_segment
    
from player_vs_segment
group by player_value_tier
order by player_value_tier;
