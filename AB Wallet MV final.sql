with value_mapping as (
    select
        date_value,
        value
    from PROD.DWH.D_DATE dd
    left join PROD.BI.POLAND_JAN_VALUE_DAYS vv
        on dd.day_in_month = vv.day
    where month_start_date = '2026-01-01'

union all

    select
        date_value,
        value
    from PROD.DWH.D_DATE dd
    left join PROD.BI.POLAND_FEB_VALUE_DAYS vv
        on dd.day_in_month = vv.day
    where month_start_date = '2026-02-01'

union all

    select
        date_value,
        value
    from PROD.DWH.D_DATE dd
    left join PROD.BI.POLAND_MAR_VALUE_DAYS vv
        on dd.day_in_month = vv.day
    where month_start_date = '2026-03-01'
    
),

segment as (
    select distinct 
        player_id,
        segmentation_month,
        ACTUAL_OFFICIAL_SEGMENTATION as player_value_tier
    from PROD.DWH.D_PLAYER
    join PROD.BI.VW_PLAYER_MONTHLY_SEGMENTATION_ALL sgm
        using (player_id)
    where is_valid = 1 
        and business_line_id = 1 
        and business_domain_id = 3
        and is_test_account <> 1
        and segmentation_month in ('2026-01-01', '2026-02-01','2026-03-01','2026-04-01')
        and player_value_tier in ('Medium Value')

        /*union all

     select distinct 
        player_id,
        '2026-03-01' as segmentation_month,
        value_tier_name as player_value_tier
    
    from PROD.DWH.D_PLAYER
    where is_valid = 1 
        and business_line_id = 1 
        and business_domain_id = 3
        and is_test_account <> 1
        and value_tier_name in ('Medium Value')*/
),

ticket_stake as (
    select
        ticket_code,
        ss.player_id,
        ss.player_value_tier,
        ss.segmentation_month,
        date_trunc(day, accepted_dt) as accepted_dt_local,
        max(stake_total_amt) as stake
    from PROD.DWH.F_TICKET ff
    join segment ss 
        on ss.player_id = ff.player_id
        and date_trunc('month', ff.accepted_dt) = ss.segmentation_month
    where business_domain_id = 3 
        and accepted_dt >= '2026-01-01'
        and accepted_dt <= '2026-03-01'
        and lower(ticket_status) <> 'cancelled'
        and stake_total_amt > 0
    group by all
),

-- MV segment-level ESI
mv_segment_esi as (
    select
        round(sum(case when vm.value = 'high' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as mv_segment_esi_high,
        round(sum(case when vm.value = 'medium' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as mv_segment_esi_mid,
        round(sum(case when vm.value = 'low' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as mv_segment_esi_low
    from ticket_stake ts
    left join value_mapping vm
        on ts.accepted_dt_local = vm.date_value
    where ts.player_value_tier = 'Medium Value'
),

-- Player ESI calculated across both months
player_esi as (
    select
        ts.player_id,
        sum(ts.stake) as total_stake,
        median(ts.stake) as median_stake,
        count(distinct ts.accepted_dt_local) as active_days,
        round(sum(case when vm.value = 'high' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as player_esi_high,
        round(sum(case when vm.value = 'medium' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as player_esi_mid,
        round(sum(case when vm.value = 'low' then ts.stake else 0 end) / nullif(sum(ts.stake), 0), 4) as player_esi_low
    from ticket_stake ts
    left join value_mapping vm
        on ts.accepted_dt_local = vm.date_value
    where ts.player_value_tier in ('Medium Value')
    group by ts.player_id  -- no longer grouping by tier, deduplication happens next
),

-- Assign latest (Feb) value tier per player to avoid duplications
player_latest_tier as (
    select
        pe.player_id,
        s.player_value_tier,  -- feb tier
        pe.total_stake,
        pe.median_stake,
        pe.active_days,
        pe.player_esi_high,
        pe.player_esi_mid,
        pe.player_esi_low
    from player_esi pe
    join segment s
        on s.player_id = pe.player_id
        and s.segmentation_month = '2026-04-01'
    where s.player_value_tier in ('Medium Value')
),

player_quintiles as (
    select
        player_id,
        player_value_tier,
        total_stake,
        median_stake,
        active_days,
        case when active_days < 10 then 1 else 0 end as low_activity_flag,
        player_esi_high,
        player_esi_mid,
        player_esi_low,
        ntile(5) over (partition by player_value_tier order by median_stake) as stake_quintile
    from player_latest_tier
),

wallet_sharing_flags as (
    select
        pq.player_id,
        pq.player_value_tier,
        pq.stake_quintile,
        pq.total_stake,
        pq.active_days,
        pq.low_activity_flag,
        pq.player_esi_high,
        pq.player_esi_mid,
        pq.player_esi_low,
        ms.mv_segment_esi_high,
        ms.mv_segment_esi_mid,
        ms.mv_segment_esi_low,
        0 as hv_segment_esi_high,
        0 as hv_segment_esi_mid,
        0 as hv_segment_esi_low,

        -- MV conditions: only populated for MV players
        case when pq.player_value_tier = 'Medium Value' 
              and pq.player_esi_high < ms.mv_segment_esi_high 
             then 1 else 0 end as mv_meets_high_condition,
        case when pq.player_value_tier = 'Medium Value' 
              and (pq.player_esi_low >= ms.mv_segment_esi_low 
                or pq.player_esi_mid >= ms.mv_segment_esi_mid)
             then 1 else 0 end as mv_meets_mid_low_condition,

        0 as hv_meets_mid_low_condition,
        0 as hv_meets_high_condition,
    from player_quintiles pq
    cross join mv_segment_esi ms
),

-- append to the end of the existing query, replacing final select

eligible_players as (
    select
        player_id,
        player_value_tier,
        stake_quintile,
        active_days,
        low_activity_flag,
        player_esi_high,
        player_esi_mid,
        player_esi_low
    from wallet_sharing_flags
    where player_value_tier = 'Medium Value'
        and low_activity_flag = 0
        and player_esi_high <= 0.4164--0.4163
        and (player_esi_mid >= 0.3767 or player_esi_low >= 0.1840)
),

-- Stratify by quintile and activity band to ensure balance
stratified as (
    select
        player_id,
        stake_quintile,
        active_days,
        low_activity_flag,
        -- activity band for balancing
        case 
            when active_days < 10 then 'low'
            when active_days between 10 and 30 then 'mid'
            else 'high'
        end as activity_band,
        -- random rank within each stratum
        row_number() over (
            partition by stake_quintile,
            case 
                when active_days < 10 then 'low'
                when active_days between 10 and 30 then 'mid'
                else 'high'
            end
            order by random()
        ) as rn,
        count(*) over (
            partition by stake_quintile,
            case 
                when active_days < 10 then 'low'
                when active_days between 10 and 30 then 'mid'
                else 'high'
            end
        ) as stratum_total
    from eligible_players
),

-- Assign test/control within each stratum (50/50 split)
assigned as (
    select
        player_id,
        stake_quintile,
        activity_band,
        active_days,
        low_activity_flag,
        rn,
        stratum_total,
        case 
            when rn <= round(stratum_total * 0.5) then 'test'
            else 'control'
        end as ab_group
    from stratified
),

-- Cap total to 1800, maintaining proportional stratum representation
final_selection as (
    select
        player_id,
        stake_quintile,
        activity_band,
        active_days,
        low_activity_flag,
        ab_group,
        row_number() over (partition by ab_group order by random()) as final_rn
    from assigned
),

fin as (
select
    ff.player_id,
    stake_quintile,
    activity_band,
    active_days,
    low_activity_flag,
    ab_group
from final_selection ff
where final_rn <= 723  -- 900 per group = 1800 total
and ff.player_id not in (select distinct player_id from PROD.BI.PIOTR_COMPARISON)
order by ab_group, stake_quintile, activity_band
)



select
*
from fin 
where player_id in (

with fin as (
select 
    player_id,
    ab_group
from PROD.BI.AB_PL_EXCLUDE 
    join PROD.BI.VW_PLAYER_MONTHLY_SEGMENTATION_ALL sgm
        using (player_id)
    where 
         --business_domain_id = 3
     segmentation_month in ('2026-04-01')
        and ACTUAL_OFFICIAL_SEGMENTATION in ('Medium Value')
and ab_group is not null
)
select * from fin
union all

select
    player_id,
    null                            as ab_group,
    
    from PROD.BI.VW_PLAYER_MONTHLY_SEGMENTATION_ALL sgm
    where 
         business_domain_code = 'SB_PL'
        and segmentation_month in ('2026-04-01')
        and ACTUAL_OFFICIAL_SEGMENTATION in ('Medium Value')
        and player_id not in (select player_id from fin group by all)
