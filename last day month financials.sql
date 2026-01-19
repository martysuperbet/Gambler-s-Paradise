-- last day of month financials
-- only last day, per month, lv cumulative each month

with players as (
    select 
        player_id,
        username
    from PROD.BI.OLA_LIST
),
daily_metrics as (
    select 
        fppad.reporting_date,
        fppad.player_id,
        p.username,
        sum(fppad.NGP) as NGP,
        sum(fppad.NGR) as NGR,
        sum(fppad.GGR) as GGR,
        last_day(fppad.reporting_date) as month_end_date,
        date_trunc('month', fppad.reporting_date) as month_start
    from PROD.DM_PLAYER.F_PLAYER_PRODUCT_PERFORMANCE_DAILY fppad
    inner join players p on fppad.player_id = p.player_id
    where fppad.business_domain_id = 3
    group by fppad.reporting_date, fppad.player_id, p.username
),
monthly_totals as (
    select
        player_id,
        username,
        month_end_date,
        -- Total for the entire month (all days)
        sum(NGP) as monthly_total_NGP,
        sum(NGR) as monthly_total_NGR,
        sum(GGR) as monthly_total_GGR
    from daily_metrics
    group by player_id, username, month_end_date
),
last_day_metrics as (
    select
        player_id,
        username,
        reporting_date as month_end_date,
        NGP as last_day_NGP,
        NGR as last_day_NGR,
        GGR as last_day_GGR
    from daily_metrics
    where reporting_date = month_end_date
),
combined as (
    select
        l.month_end_date,
        l.player_id,
        l.username,
        -- 1) Last day of month metrics
        l.last_day_NGP,
        l.last_day_NGR,
        l.last_day_GGR,
        -- 2) Total of entire month
        m.monthly_total_NGP,
        m.monthly_total_NGR,
        m.monthly_total_GGR,
        -- 3) Cumulative all-time up to last day of month
        sum(m.monthly_total_NGP) over (partition by l.player_id order by l.month_end_date) as cumulative_NGP,
        sum(m.monthly_total_NGR) over (partition by l.player_id order by l.month_end_date) as cumulative_NGR,
        sum(m.monthly_total_GGR) over (partition by l.player_id order by l.month_end_date) as cumulative_GGR
    from last_day_metrics l
    inner join monthly_totals m 
        on l.player_id = m.player_id 
        and l.month_end_date = m.month_end_date
)
select
    month_end_date,
    player_id,
    username,
    last_day_NGP,
    last_day_NGR,
    last_day_GGR,
    monthly_total_NGP,
    monthly_total_NGR,
    monthly_total_GGR,
    cumulative_NGP,
    cumulative_NGR,
    cumulative_GGR
from combined
where month_end_date > '2025-01-01'
order by player_id, month_end_date;
