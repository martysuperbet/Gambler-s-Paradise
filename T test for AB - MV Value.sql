with segment as (
select * from PROD.BI.POLAND_AB_TEST_ACCELERATING_MV
),
d_date as (
select date_value as reporting_date
from PROD.DWH.D_DATE
where date_trunc(month,date_value) >= '2026-02-01'
and date_trunc(month,date_value) <= '2026-03-15'
),
ticket_stake as (
    select
        ticket_code,
        ss.test_group,
        ss.player_id,
        date_trunc(day, accepted_dt) as accepted_dt_local,
        max(stake_total_amt*fer1.MIDDLE_RATE) as stake
    from PROD.DWH.F_TICKET ff
    join segment ss on ss.player_id = ff.player_id
    LEFT JOIN DWH.F_EXCHANGE_RATE fer1 
        ON fer1.FROM_CURRENCY_ID = ff.CURRENCY_ID 
        AND fer1.RATE_DATE = date_trunc(day, ff.accepted_dt)
        AND fer1.TO_CURRENCY_CODE = 'EUR'
    where business_domain_id = 3 
        and accepted_dt >= '2026-02-01'
        and lower(ticket_status) <> 'cancelled'
        and stake_total_amt > 0
    group by all
),
player_stake as (
    select
        player_id,
        test_group,
        count(distinct ticket_code)                         as total_tickets,
        count(distinct accepted_dt_local)                   as active_days,
        sum(stake)                                          as total_stake,
        avg(stake)                                          as avg_stake_per_ticket,
        median(stake)                                       as median_stake_per_ticket,
        sum(stake) / count(distinct accepted_dt_local)      as avg_daily_stake
    from ticket_stake
    group by player_id, test_group
),
deps as (
select
    player_id,
    sum(deposit_amount*fer1.MIDDLE_RATE)    as total_deposit_eur,
    sum(deposit_count)                      as total_deposit_count
from PROD.DM_PLAYER.F_PLAYER_PAYMENTS_DAILY dd
LEFT JOIN DWH.F_EXCHANGE_RATE fer1 
    ON fer1.FROM_CURRENCY_ID = dd.CURRENCY_ID 
    AND fer1.RATE_DATE = dd.reporting_date
    AND fer1.TO_CURRENCY_CODE = 'EUR'
WHERE business_domain_id = 3
and date_trunc(month,reporting_date) >= '2026-02-01'
group by player_id
),
financials as (
select
    ss.player_id,
    ss.test_group,
    sum(coalesce(ngr*fer1.MIDDLE_RATE, 0))          as total_ngr,
    sum(coalesce(bet_total*fer1.MIDDLE_RATE, 0))    as total_turnover,
    sum(coalesce(bonus_cost*fer1.MIDDLE_RATE, 0))   as total_bonus_cost,
    count(distinct case when ff.player_id is not null 
        then dt.reporting_date end)                 as active_days
from segment ss
cross join d_date dt
left join PROD.DM_PLAYER.F_PLAYER_PRODUCT_PERFORMANCE_DAILY ff
    on ss.player_id = ff.player_id
    and dt.reporting_date = ff.reporting_date
    and business_domain_id = 3
    and date_trunc(month,ff.reporting_date) >= '2026-02-01'
LEFT JOIN DWH.F_EXCHANGE_RATE fer1 
    ON fer1.FROM_CURRENCY_ID = ff.CURRENCY_ID 
    AND fer1.RATE_DATE = dt.reporting_date
    AND fer1.TO_CURRENCY_CODE = 'EUR'
group by ss.player_id, ss.test_group
),
player_level as (
    select
        f.player_id,
        f.test_group,
        f.total_ngr,
        f.total_turnover,
        f.total_bonus_cost,
        f.active_days,
        coalesce(d.total_deposit_eur, 0)            as total_deposits,
        coalesce(d.total_deposit_count, 0)          as total_deposit_count,
        coalesce(ps.avg_stake_per_ticket, 0)        as avg_stake_per_ticket,
        coalesce(ps.median_stake_per_ticket, 0)     as median_stake_per_ticket,
        coalesce(ps.avg_daily_stake, 0)             as avg_daily_stake,
        coalesce(ps.total_tickets, 0)               as total_tickets
    from financials f
    left join deps d          on f.player_id = d.player_id
    left join player_stake ps on f.player_id = ps.player_id
),
group_stats as (
    select
        test_group,
        count(distinct player_id)                                       as total_players,
        count(distinct case when active_days > 0 then player_id end)   as total_actives,

        -- netARPU
        sum(total_ngr) / nullif(count(distinct case when active_days > 0
            then player_id end), 0)                                     as net_arpu,
        avg(total_ngr)                                                  as mean_ngr,
        variance(total_ngr)                                             as var_ngr,

        -- Avg stake per ticket
        avg(avg_stake_per_ticket)                                       as mean_avg_stake,
        variance(avg_stake_per_ticket)                                  as var_avg_stake,

        -- Median stake per ticket (directional only)
        median(median_stake_per_ticket)                                 as median_stake,

        -- Deposits amount
        avg(total_deposits)                                             as mean_deposits,
        variance(total_deposits)                                        as var_deposits,

        -- Deposit count
        avg(total_deposit_count)                                        as mean_deposit_count,
        variance(total_deposit_count)                                   as var_deposit_count,

        -- Active days
        avg(active_days)                                                as mean_active_days,
        variance(active_days)                                           as var_active_days,

        -- Turnover
        avg(total_turnover)                                             as mean_turnover,
        variance(total_turnover)                                        as var_turnover,

        -- Tickets per player (frequency guardrail)
        avg(total_tickets)                                              as mean_tickets,
        variance(total_tickets)                                         as var_tickets

    from player_level
    group by test_group
),
test_grp as (select * from group_stats where test_group = 'TEST'),
ctrl_grp as (select * from group_stats where test_group = 'CONTROL')

select
    -- Sample sizes
    t.total_players                                                     as test_players,
    c.total_players                                                     as ctrl_players,
    t.total_actives                                                     as test_actives,
    c.total_actives                                                     as ctrl_actives,

    -- netARPU
    round(t.net_arpu, 4)                                               as test_net_arpu,
    round(c.net_arpu, 4)                                               as ctrl_net_arpu,
    round((t.net_arpu - c.net_arpu) / nullif(c.net_arpu,0) * 100, 2)  as net_arpu_uplift_pct,
    round((t.mean_ngr - c.mean_ngr)
        / nullif(sqrt(t.var_ngr/t.total_players
        + c.var_ngr/c.total_players), 0), 4)                           as net_arpu_t_stat,

    -- Avg stake per ticket (core hypothesis metric)
    round(t.mean_avg_stake, 4)                                         as test_avg_stake,
    round(c.mean_avg_stake, 4)                                         as ctrl_avg_stake,
    round((t.mean_avg_stake - c.mean_avg_stake)
        / nullif(c.mean_avg_stake,0) * 100, 2)                         as avg_stake_uplift_pct,
    round((t.mean_avg_stake - c.mean_avg_stake)
        / nullif(sqrt(t.var_avg_stake/t.total_players
        + c.var_avg_stake/c.total_players), 0), 4)                     as avg_stake_t_stat,

    -- Median stake (directional only, no t-stat)
    round(t.median_stake, 4)                                           as test_median_stake,
    round(c.median_stake, 4)                                           as ctrl_median_stake,
    round((t.median_stake - c.median_stake)
        / nullif(c.median_stake,0) * 100, 2)                           as median_stake_uplift_pct,

    -- Deposits amount
    round(t.mean_deposits, 4)                                          as test_avg_deposit,
    round(c.mean_deposits, 4)                                          as ctrl_avg_deposit,
    round((t.mean_deposits - c.mean_deposits)
        / nullif(c.mean_deposits,0) * 100, 2)                          as deposit_uplift_pct,
    round((t.mean_deposits - c.mean_deposits)
        / nullif(sqrt(t.var_deposits/t.total_players
        + c.var_deposits/c.total_players), 0), 4)                      as deposit_t_stat,

    -- Deposit count
    round(t.mean_deposit_count, 4)                                     as test_avg_deposit_count,
    round(c.mean_deposit_count, 4)                                     as ctrl_avg_deposit_count,
    round((t.mean_deposit_count - c.mean_deposit_count)
        / nullif(c.mean_deposit_count,0) * 100, 2)                     as deposit_count_uplift_pct,
    round((t.mean_deposit_count - c.mean_deposit_count)
        / nullif(sqrt(t.var_deposit_count/t.total_players
        + c.var_deposit_count/c.total_players), 0), 4)                 as deposit_count_t_stat,

    -- Active days (guardrail)
    round(t.mean_active_days, 4)                                       as test_avg_active_days,
    round(c.mean_active_days, 4)                                       as ctrl_avg_active_days,
    round((t.mean_active_days - c.mean_active_days)
        / nullif(c.mean_active_days,0) * 100, 2)                       as active_days_uplift_pct,
    round((t.mean_active_days - c.mean_active_days)
        / nullif(sqrt(t.var_active_days/t.total_players
        + c.var_active_days/c.total_players), 0), 4)                   as active_days_t_stat,

    -- Turnover (guardrail)
    round(t.mean_turnover, 4)                                          as test_avg_turnover,
    round(c.mean_turnover, 4)                                          as ctrl_avg_turnover,
    round((t.mean_turnover - c.mean_turnover)
        / nullif(c.mean_turnover,0) * 100, 2)                          as turnover_uplift_pct,
    round((t.mean_turnover - c.mean_turnover)
        / nullif(sqrt(t.var_turnover/t.total_players
        + c.var_turnover/c.total_players), 0), 4)                      as turnover_t_stat,

    -- Ticket frequency (guardrail)
    round(t.mean_tickets, 4)                                           as test_avg_tickets,
    round(c.mean_tickets, 4)                                           as ctrl_avg_tickets,
    round((t.mean_tickets - c.mean_tickets)
        / nullif(c.mean_tickets,0) * 100, 2)                           as tickets_uplift_pct,
    round((t.mean_tickets - c.mean_tickets)
        / nullif(sqrt(t.var_tickets/t.total_players
        + c.var_tickets/c.total_players), 0), 4)                       as tickets_t_stat,

    -- 95% CI on netARPU difference
    round((t.mean_ngr - c.mean_ngr)
        - 1.96 * sqrt(t.var_ngr/t.total_players
        + c.var_ngr/c.total_players), 4)                               as net_arpu_ci_lower,
    round((t.mean_ngr - c.mean_ngr)
        + 1.96 * sqrt(t.var_ngr/t.total_players
        + c.var_ngr/c.total_players), 4)                               as net_arpu_ci_upper

from test_grp t, ctrl_grp c;
