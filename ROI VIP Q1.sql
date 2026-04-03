

/*
╔══════════════════════════════════════════════════════════════════════════════════╗
║  QUERY 1 — POINTS RANK (RANKING PUNKTOWY)                                       ║
║  Turnover Uplift + ROI vs Trip Cost                                              ║
║                                                                                  ║
║  London  FEB25  : promotion 18.02.2025 – 18.03.2025 (28 days)                  ║
║                   baseline  21.01.2025 – 17.02.2025 (28 days)  cost: 254000 PLN ║
║  Madrid  SEPT25 : promotion 19.09.2025 – 26.10.2025 (37 days)                  ║
║                   baseline  13.08.2025 – 18.09.2025 (37 days)  cost: 201600 PLN ║
║                                                                                  ║
║  Methodology: Difference-in-Differences (DiD)                                   ║
║    · Baseline  = same-length window immediately before each promotion            ║
║    · Treatment = promotion window                                                ║
║    · Control   = High Value tier, VIP participants excluded                      ║
║                                                                                  ║
║  ROI note: roi_gross_turnover_ratio uses raw stake (turnover).                  ║
║            For revenue-based ROI multiply total_incremental_turnover             ║
║            by your GGR margin % before dividing by trip_cost_pln.              ║
╚══════════════════════════════════════════════════════════════════════════════════╝
*/
 
WITH
 
/* ── 1. TRIP COST & PERIOD CONSTANTS ──────────────────────────────────────── */
trip_costs AS (
    SELECT
        'FEB25'             AS ranking,
        DATE '2025-01-21'   AS baseline_start,
        DATE '2025-02-17'   AS baseline_end,
        DATE '2025-02-18'   AS treatment_start,
        DATE '2025-03-18'   AS treatment_end,
        '2025-02-01'        AS segmentation_month,
        254000              AS trip_cost_pln
    UNION ALL
    SELECT
        'SEPT25',
        DATE '2025-08-13',
        DATE '2025-09-18',
        DATE '2025-09-19',
        DATE '2025-10-26',
        '2025-09-01',
        201600
),
 
/* ── 2. VIP PLAYERS — Points Ranking ─────────────────────────────────────── */
vip_points AS (
    SELECT CAST(pp.player_id AS VARCHAR) as player_id,
    'FEB25'  AS ranking
    
    FROM PROD.BI.VIP_POINTS_RANK_FEB25 feb
    left join PROD.DWH.D_PLAYER pp
        on CAST(feb.player_id AS VARCHAR) = pp.player_external_code
    where business_domain_id = 3
    group by all

    UNION
    
    SELECT CAST(player_id AS VARCHAR),               'SEPT25'
    FROM PROD.BI.VIP_POINTS_RANKING_SEPT25
),
 
/* ── 3. HIGH VALUE CONTROL GROUP ─────────────────────────────────────────── */
hv_players AS (
    SELECT DISTINCT
        CAST(sgm.player_id AS VARCHAR) AS player_id,
        tc.ranking
    FROM PROD.BI.VW_PLAYER_MONTHLY_SEGMENTATION_ALL sgm
    JOIN trip_costs tc
        ON sgm.segmentation_month = tc.segmentation_month
    WHERE sgm.business_domain_code             = 'SB_PL'
        AND sgm.ACTUAL_OFFICIAL_SEGMENTATION   = 'High Value'
        AND CAST(sgm.player_id AS VARCHAR) NOT IN (SELECT 
pp.player_id,
FROM PROD.BI.VIP_POINTS_RANK_FEB25 feb
left join PROD.DWH.D_PLAYER pp
    on CAST(feb.player_id AS VARCHAR) = pp.player_external_code
where business_domain_id = 3
group by all)
        AND CAST(sgm.player_id AS VARCHAR) NOT IN (SELECT CAST(player_id AS VARCHAR) FROM PROD.BI.VIP_POINTS_RANKING_SEPT25)
),
 
/* ── 4. UNIFIED PLAYER UNIVERSE ──────────────────────────────────────────── */
all_players AS (
    SELECT player_id, ranking, 'VIP'        AS player_group FROM vip_points
    UNION ALL
    SELECT player_id, ranking, 'High Value'                  FROM hv_players
),
 
/* ── 5. RAW TICKET DATA ───────────────────────────────────────────────────── */
tickets_raw AS (
    SELECT
        CAST(player_id AS VARCHAR)                                                  AS player_id,
        accepted_dt                                                                 AS reporting_date,
        ticket_code,
        TRY_CAST(REPLACE(stake_total_amt::VARCHAR, ',', '') AS FLOAT)               AS stake_total_amt,
        TRY_CAST(REPLACE(ticket_selection_coefficient_sum::VARCHAR, ',', '') AS FLOAT)
                                                                                    AS ticket_selection_coefficient_sum
    FROM PROD.DWH.F_TICKET
    WHERE business_domain_id = 3
        AND accepted_dt BETWEEN '2025-01-21' AND '2025-10-26'
),
 
/* ── 6. PER-PLAYER METRICS: BASELINE vs TREATMENT ─────────────────────────── */
player_metrics AS (
    SELECT
        ap.player_id,
        ap.ranking,
        ap.player_group,
        tc.trip_cost_pln,
 
        -- BASELINE
        COALESCE(SUM(CASE WHEN t.reporting_date BETWEEN tc.baseline_start AND tc.baseline_end
                          THEN t.stake_total_amt END), 0)                AS baseline_stake,
        COUNT(DISTINCT CASE WHEN t.reporting_date BETWEEN tc.baseline_start AND tc.baseline_end
                            THEN t.ticket_code END)                      AS baseline_tickets,
        AVG(CASE WHEN t.reporting_date BETWEEN tc.baseline_start AND tc.baseline_end
                 THEN t.ticket_selection_coefficient_sum END)            AS baseline_avg_odds,
 
        -- TREATMENT
        COALESCE(SUM(CASE WHEN t.reporting_date BETWEEN tc.treatment_start AND tc.treatment_end
                          THEN t.stake_total_amt END), 0)                AS treatment_stake,
        COUNT(DISTINCT CASE WHEN t.reporting_date BETWEEN tc.treatment_start AND tc.treatment_end
                            THEN t.ticket_code END)                      AS treatment_tickets,
        AVG(CASE WHEN t.reporting_date BETWEEN tc.treatment_start AND tc.treatment_end
                 THEN t.ticket_selection_coefficient_sum END)            AS treatment_avg_odds
 
    FROM all_players ap
    JOIN trip_costs     tc ON ap.ranking    = tc.ranking
    LEFT JOIN tickets_raw t ON ap.player_id = t.player_id
    GROUP BY ap.player_id, ap.ranking, ap.player_group, tc.trip_cost_pln
),
 
/* ── 7. GROUP-LEVEL SUMMARY ───────────────────────────────────────────────── */
group_summary AS (
    SELECT
        ranking,
        player_group,
        MAX(trip_cost_pln)                         AS trip_cost_pln,
        COUNT(DISTINCT player_id)                  AS player_count,
        AVG(baseline_stake)                        AS avg_baseline_stake,
        AVG(treatment_stake)                       AS avg_treatment_stake,
        AVG(treatment_stake - baseline_stake)      AS avg_stake_change,
        AVG(baseline_tickets)                      AS avg_baseline_tickets,
        AVG(treatment_tickets)                     AS avg_treatment_tickets,
        AVG(baseline_avg_odds)                     AS avg_baseline_odds,
        AVG(treatment_avg_odds)                    AS avg_treatment_odds,
        SUM(baseline_stake)                        AS total_baseline_stake,
        SUM(treatment_stake)                       AS total_treatment_stake
    FROM player_metrics
    GROUP BY ranking, player_group
)
 
/* ── 8. FINAL OUTPUT: DiD + ROI ───────────────────────────────────────────── */
SELECT
    vip.ranking,
    vip.trip_cost_pln,
 
    /* ── VIP Group ─────────────────────────────────────────────────────── */
    vip.player_count                                                                   AS vip_player_count,
    ROUND(vip.avg_baseline_stake,     2)                                               AS vip_avg_baseline_stake,
    ROUND(vip.avg_treatment_stake,    2)                                               AS vip_avg_treatment_stake,
    ROUND(vip.avg_stake_change,       2)                                               AS vip_avg_stake_change,
    ROUND(100.0 * vip.avg_stake_change / NULLIF(vip.avg_baseline_stake, 0), 2)         AS vip_stake_change_pct,
    ROUND(vip.avg_baseline_tickets,   1)                                               AS vip_avg_baseline_tickets,
    ROUND(vip.avg_treatment_tickets,  1)                                               AS vip_avg_treatment_tickets,
    ROUND(vip.avg_baseline_odds,      2)                                               AS vip_avg_baseline_odds,
    ROUND(vip.avg_treatment_odds,     2)                                               AS vip_avg_treatment_odds,
 
    /* ── High Value Control Group ──────────────────────────────────────── */
    hv.player_count                                                                    AS hv_player_count,
    ROUND(hv.avg_baseline_stake,      2)                                               AS hv_avg_baseline_stake,
    ROUND(hv.avg_treatment_stake,     2)                                               AS hv_avg_treatment_stake,
    ROUND(hv.avg_stake_change,        2)                                               AS hv_avg_stake_change,
    ROUND(100.0 * hv.avg_stake_change / NULLIF(hv.avg_baseline_stake, 0), 2)           AS hv_stake_change_pct,
    ROUND(hv.avg_baseline_tickets,    1)                                               AS hv_avg_baseline_tickets,
    ROUND(hv.avg_treatment_tickets,   1)                                               AS hv_avg_treatment_tickets,
    ROUND(hv.avg_baseline_odds,       2)                                               AS hv_avg_baseline_odds,
    ROUND(hv.avg_treatment_odds,      2)                                               AS hv_avg_treatment_odds,
 
    /* ── DiD: Incremental lift attributed to ranking ───────────────────── */
    ROUND(vip.avg_stake_change - hv.avg_stake_change, 2)                               AS did_stake_uplift_per_player,
    ROUND(100.0 * (vip.avg_stake_change - hv.avg_stake_change)
          / NULLIF(hv.avg_baseline_stake, 0), 2)                                       AS did_stake_uplift_pct,
 
    /* ── Total incremental turnover = DiD per player x VIP count ────────── */
    ROUND((vip.avg_stake_change - hv.avg_stake_change) * vip.player_count, 2)          AS total_incremental_turnover_pln,
 
    /* ── ROI vs Trip Cost ───────────────────────────────────────────────── */
    ROUND(
        ((vip.avg_stake_change - hv.avg_stake_change) * vip.player_count)
        / NULLIF(vip.trip_cost_pln, 0), 2
    )                                                                                   AS roi_gross_turnover_ratio,
 
    CASE
        WHEN (vip.avg_stake_change - hv.avg_stake_change) * vip.player_count
             >= vip.trip_cost_pln THEN 'POSITIVE (gross turnover basis)'
        ELSE                           'NEGATIVE (gross turnover basis)'
    END                                                                                 AS roi_verdict
 
FROM group_summary vip
JOIN group_summary hv
    ON  vip.ranking      = hv.ranking
    AND vip.player_group = 'VIP'
    AND hv.player_group  = 'High Value'
 
ORDER BY vip.ranking
;
