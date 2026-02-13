-- Simplified ESI Calculation
-- 1. ESI for each value type day
-- 2. ESI per value type per player tier

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

stake_by_value_type as (
    select
        ts.player_value_tier,
        sum(case when vm.value = 'high' then ts.stake else 0 end) as stake_high_value_days,
        sum(case when vm.value = 'medium' then ts.stake else 0 end) as stake_mid_value_days,
        sum(case when vm.value = 'low' then ts.stake else 0 end) as stake_low_value_days,
        sum(ts.stake) as total_stake
    from ticket_stake ts
    left join value_mapping vm
        on ts.accepted_dt_local = vm.date_value
    group by all
)

-- Calculate ESI per player tier
select
    player_value_tier,
    total_stake,
    stake_high_value_days,
    stake_mid_value_days,
    stake_low_value_days,
    round(stake_high_value_days / nullif(total_stake, 0), 4) as esi_high,
    round(stake_mid_value_days / nullif(total_stake, 0), 4) as esi_mid,
    round(stake_low_value_days / nullif(total_stake, 0), 4) as esi_low
from stake_by_value_type
group by all
--order by player_value_tier;
