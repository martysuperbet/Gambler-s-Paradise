-- Wallet-Sharing Analysis for High Value Players
-- Identify HV players per quintile who:
-- 1. Have lower ESI high-value days than HV segment overall
-- 2. Have higher or equal ESI in low-value AND mid-value days than in high-value days

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
    group by all
),

-- Calculate HV segment-level ESI
hv_segment_esi as (
    select
        round(sum(case when vm.value = 'high' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as hv_segment_esi_high,
        round(sum(case when vm.value = 'medium' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as hv_segment_esi_mid,
        round(sum(case when vm.value = 'low' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as hv_segment_esi_low
    from ticket_stake ts
    left join value_mapping vm
        on ts.accepted_dt_local = vm.date_value
    where ts.player_value_tier = 'High Value'  -- Only HV segment
),

-- Calculate player-level ESI and total stake
player_esi as (
    select
        ts.player_id,
        ts.player_value_tier,
        sum(ts.stake) as total_stake,
        median(ts.stake) as median_stake,
        round(sum(case when vm.value = 'high' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as player_esi_high,
        round(sum(case when vm.value = 'medium' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as player_esi_mid,
        round(sum(case when vm.value = 'low' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as player_esi_low
    from ticket_stake ts
    left join value_mapping vm
        on ts.accepted_dt_local = vm.date_value
    where ts.player_value_tier = 'High Value'  -- Only HV players
    group by ts.player_id, ts.player_value_tier
),

-- Assign quintiles based on total stake (quintile 5 = highest stake)
player_quintiles as (
    select
        player_id,
        player_value_tier,
        total_stake,
        median_stake,
        player_esi_high,
        player_esi_mid,
        player_esi_low,
        ntile(5) over (order by median_stake) as stake_quintile
    from player_esi
),

  -- players who declared they are with other competitors 
ankieta as (
with players as (
select distinct
    ii.*,
    username,
    player_id
from PROD.BI.IGOR_ANKIETA ii
left join (select
            username,
            player_id,
            from PROD.DWH.D_PLAYER dd
            where business_domain_id = 3) dd
    on lower(ii.login_corrected) = lower(dd.username)
where player_id is not null
and lower(other_sportsbetting) = 'tak'

    union all 
    
select distinct
    ii.*,
    username,
    player_id
from PROD.BI.IGOR_ANKIETA ii
left join (select
            username,
            email,
            player_id,
            from PROD.DWH.D_PLAYER dd
            where business_domain_id = 3) dd
    on lower(ii.login_corrected) = lower(dd.email)
where player_id is not null
and lower(other_sportsbetting) = 'tak'

union all

select distinct
    ii.*,
    username,
    player_id
from PROD.BI.IGOR_ANKIETA_2 ii
left join (select
            username,
            email,
            player_id,
            from PROD.DWH.D_PLAYER dd
            where business_domain_id = 3) dd
    on lower(ii.email) = lower(dd.email)
where player_id is not null
and lower(other_sportsbetting) = 'tak'
),

attr as (
select distinct
    pp.player_id,
    ACTUAL_OFFICIAL_SEGMENTATION as monthly_official_segmentation,
    floor(datediff('day',registration_dt, current_date())) as customer_longevity,
    
from players pp
left join PROD.DWH.D_PLAYER dd
    on pp.player_id = dd.player_id
left join PROD.BI.VW_PLAYER_MONTHLY_SEGMENTATION_ALL sgm
    on pp.player_id = sgm.Player_id
where business_domain_id = 3
    and segmentation_month = '2026-02-01'
group by all
)

select 
player_id
from attr
where monthly_official_segmentation = 'High Value'
group by all
),

-- Flag conditions separately
wallet_sharing_flags as (
    select
        pq.player_id,
        pq.player_value_tier,
        pq.stake_quintile,
        pq.total_stake,
        pq.player_esi_high,
        pq.player_esi_mid,
        pq.player_esi_low,
        hs.hv_segment_esi_high,
        hs.hv_segment_esi_mid,
        hs.hv_segment_esi_low,
        -- Separate condition flags
        case when pq.player_esi_high < hs.hv_segment_esi_high then 1 else 0 end as meets_high_condition,
        case when pq.player_esi_low >= hs.hv_segment_esi_low 
              or pq.player_esi_mid >= hs.hv_segment_esi_mid 
             then 1 else 0 end as meets_mid_low_condition
    from player_quintiles pq
    cross join hv_segment_esi hs
    where pq.player_id in (select player_id from ankieta group by all)
)
select * from wallet_sharing_flags

-- Count distinct players per quintile by condition
select
    stake_quintile,
    count(distinct player_id) as total_players,
    --median(total_stake) as median_stake,
    -- Players meeting high condition
    count(distinct case when meets_high_condition = 1 then player_id end) as players_high_condition,
    round(count(distinct case when meets_high_condition = 1 then player_id end) * 100.0 / count(distinct player_id), 2) as pct_high_condition,
    
    -- Players meeting mid/low condition
    count(distinct case when meets_mid_low_condition = 1 then player_id end) as players_mid_low_condition,
    round(count(distinct case when meets_mid_low_condition = 1 then player_id end) * 100.0 / count(distinct player_id), 2) as pct_mid_low_condition,
    
    -- Players meeting BOTH conditions (wallet-sharers)
    count(distinct case when meets_high_condition = 1 and meets_mid_low_condition = 1 then player_id end) as wallet_sharers,
    round(count(distinct case when meets_high_condition = 1 and meets_mid_low_condition = 1 then player_id end) * 100.0 / count(distinct player_id), 2) as wallet_sharer_pct,
    
    -- Stake statistics
    sum(total_stake) as total_stake_eur,
    sum(case when meets_high_condition = 1 and meets_mid_low_condition = 1 then total_stake end) as wallet_sharer_stake_eur,
    
    -- HV Segment ESI reference
    max(hv_segment_esi_high) as hv_segment_esi_high,
    max(hv_segment_esi_mid) as hv_segment_esi_mid,
    max(hv_segment_esi_low) as hv_segment_esi_low

from wallet_sharing_flags
group by stake_quintile
order by stake_quintile;
