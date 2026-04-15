
--https://docs.google.com/spreadsheets/d/1cNwdFbeVhcM4mLNF0cOy801-sb8eYfoqNNSiKaY33LY/edit?gid=2741516#gid=2741516
--High-frequency customers (100+ bets in Q4)

WITH high_freq_q4 AS (
    -- Base cohort: customers with 100+ bets in Q4'25
    SELECT
        player_id,
        COUNT(DISTINCT ticket_code)                                         AS bet_count_q4
    FROM PROD.DM_SPORT.F_TICKET_SELECTION_2Y_MATERIALIZED
    WHERE business_domain_id = 3
        AND LOWER(status_reason) <> 'cancelled'
        AND date_trunc('quarter', accepted_dt_local) = '2025-10-01'
    GROUP BY player_id
    HAVING COUNT(DISTINCT ticket_code) >= 100
),

-- Ticket-level dedup to avoid multiplying cashout/ticket_type across selections
ticket_level AS (
    SELECT DISTINCT
        t.player_id,
        t.ticket_code,
        t.ticket_type,                  -- PREMATCH / INPLAY / MIXED
        t.status_reason,                -- WIN / LOST / CASHOUT
        t.accepted_dt_local,
        t.sport_name,
        date_trunc('quarter', t.accepted_dt_local)                          AS quarter
    FROM PROD.DM_SPORT.F_TICKET_SELECTION_2Y_MATERIALIZED t
    WHERE t.business_domain_id = 3
        AND LOWER(t.status_reason) <> 'cancelled'
        AND date_trunc('quarter', t.accepted_dt_local) IN ('2025-10-01', '2026-01-01')
        AND t.player_id IN (SELECT player_id FROM high_freq_q4)
),

-- Selection-level aggregation (stakes + odds + GGR) — safe to sum due to _w_sum weighting
selection_level AS (
    SELECT
        t.player_id,
        date_trunc('quarter', t.accepted_dt_local)                          AS quarter,
        t.ticket_type,
        t.status_reason,
        t.sport_name,
        -- Stake
        SUM(t.stake_total_amt_eur_w_sum)                                    AS to_eur,
        -- GGR proxy: stake - payout, using net stake as base
        SUM(t.stake_net_amt_eur_w_sum)                                      AS net_stake_eur,
        -- Odds: weighted average by stake
        SUM(t.selection_coefficient * t.stake_total_amt_eur_w_sum)
            / NULLIF(SUM(t.stake_total_amt_eur_w_sum), 0)                   AS avg_odds_weighted,
        -- Odds buckets
        COUNT(DISTINCT CASE WHEN t.selection_coefficient < 1.5  THEN t.ticket_code END) AS bets_odds_under_1_5,
        COUNT(DISTINCT CASE WHEN t.selection_coefficient BETWEEN 1.5 AND 3.0 THEN t.ticket_code END) AS bets_odds_1_5_to_3,
        COUNT(DISTINCT CASE WHEN t.selection_coefficient > 3.0  THEN t.ticket_code END) AS bets_odds_over_3
    FROM PROD.DM_SPORT.F_TICKET_SELECTION_2Y_MATERIALIZED t
    WHERE t.business_domain_id = 3
        AND LOWER(t.status_reason) <> 'cancelled'
        AND date_trunc('quarter', t.accepted_dt_local) IN ('2025-10-01', '2026-01-01')
        AND t.player_id IN (SELECT player_id FROM high_freq_q4)
    GROUP BY 1, 2, 3, 4, 5
),

-- Roll up to player x quarter x ticket_type (collapse sport and status into aggregates)
player_quarter AS (
    SELECT
        player_id,
        quarter,

        -- Volume
        SUM(to_eur)                                                         AS total_to_eur,
        SUM(net_stake_eur)                                                  AS total_net_stake_eur,

        -- Bet type split ($)
        SUM(CASE WHEN ticket_type = 'INPLAY'   THEN to_eur ELSE 0 END)     AS inplay_to,
        SUM(CASE WHEN ticket_type = 'PREMATCH' THEN to_eur ELSE 0 END)     AS prematch_to,
        SUM(CASE WHEN ticket_type = 'MIX'    THEN to_eur ELSE 0 END)     AS mixed_to,

        -- Settlement split ($)
        SUM(CASE WHEN status_reason = 'WIN'     THEN to_eur ELSE 0 END)    AS win_to,
        SUM(CASE WHEN status_reason = 'LOST'    THEN to_eur ELSE 0 END)    AS lost_to,
        SUM(CASE WHEN status_reason = 'CASHOUT' THEN to_eur ELSE 0 END)    AS cashout_to,

        -- Weighted avg odds
        SUM(avg_odds_weighted * to_eur)
            / NULLIF(SUM(to_eur), 0)                                        AS avg_odds_weighted,

        -- Odds bucket counts
        SUM(bets_odds_under_1_5)                                            AS bets_odds_under_1_5,
        SUM(bets_odds_1_5_to_3)                                             AS bets_odds_1_5_to_3,
        SUM(bets_odds_over_3)                                               AS bets_odds_over_3,

        -- Top sport by TO
        MAX(sport_name)                                                     AS top_sport  -- use mode workaround below if needed

    FROM selection_level
    GROUP BY 1, 2
),

-- Ticket counts and cashout rate (from deduped ticket level)
ticket_agg AS (
    SELECT
        player_id,
        quarter,
        COUNT(DISTINCT ticket_code)                                                         AS bet_count,
        COUNT(DISTINCT CASE WHEN ticket_type = 'INPLAY'   THEN ticket_code END)            AS inplay_bets,
        COUNT(DISTINCT CASE WHEN ticket_type = 'PREMATCH' THEN ticket_code END)            AS prematch_bets,
        COUNT(DISTINCT CASE WHEN ticket_type = 'MIX'    THEN ticket_code END)            AS mixed_bets,
        COUNT(DISTINCT CASE WHEN status_reason = 'CASHOUT' THEN ticket_code END)           AS cashout_bets,
        COUNT(DISTINCT CASE WHEN status_reason = 'WIN'     THEN ticket_code END)           AS win_bets,
        COUNT(DISTINCT CASE WHEN status_reason = 'LOST'    THEN ticket_code END)           AS lost_bets,
        COUNT(DISTINCT sport_name)                                                          AS distinct_sports
    FROM ticket_level
    GROUP BY 1, 2
)

-- Final output: one row per quarter, aggregated across all high-frequency customers
SELECT
    pq.quarter,

    -- Cohort size
    COUNT(DISTINCT pq.player_id)                                            AS player_count,

    -- Frequency
    SUM(ta.bet_count)                                                       AS total_bets,
    ROUND(AVG(ta.bet_count), 1)                                             AS avg_bets_per_player,
    ROUND(AVG(ta.distinct_sports), 1)                                       AS avg_distinct_sports,

    -- Total TO
    SUM(pq.total_to_eur)                                                    AS total_to_eur,
    ROUND(AVG(pq.total_to_eur), 2)                                          AS avg_to_per_player,

    -- Bet type split ($)
    SUM(pq.inplay_to)                                                       AS inplay_to,
    SUM(pq.prematch_to)                                                     AS prematch_to,
    SUM(pq.mixed_to)                                                        AS mixed_to,

    -- Bet type split (%)
    ROUND(DIV0(SUM(pq.inplay_to),   SUM(pq.total_to_eur)) * 100, 2)       AS inplay_pct,
    ROUND(DIV0(SUM(pq.prematch_to), SUM(pq.total_to_eur)) * 100, 2)       AS prematch_pct,
    ROUND(DIV0(SUM(pq.mixed_to),    SUM(pq.total_to_eur)) * 100, 2)       AS mixed_pct,

    -- Bet count split
    SUM(ta.inplay_bets)                                                     AS inplay_bets,
    SUM(ta.prematch_bets)                                                   AS prematch_bets,
    SUM(ta.mixed_bets)                                                      AS mixed_bets,
    ROUND(DIV0(SUM(ta.inplay_bets),   SUM(ta.bet_count)) * 100, 2)        AS inplay_bet_pct,
    ROUND(DIV0(SUM(ta.prematch_bets), SUM(ta.bet_count)) * 100, 2)        AS prematch_bet_pct,
    ROUND(DIV0(SUM(ta.mixed_bets),    SUM(ta.bet_count)) * 100, 2)        AS mixed_bet_pct,

    -- Settlement ($)
    SUM(pq.win_to)                                                          AS win_to,
    SUM(pq.lost_to)                                                         AS lost_to,
    SUM(pq.cashout_to)                                                      AS cashout_to,
    ROUND(DIV0(SUM(pq.cashout_to), SUM(pq.total_to_eur)) * 100, 2)        AS cashout_to_pct,

    -- Settlement (bet count)
    SUM(ta.win_bets)                                                        AS win_bets,
    SUM(ta.lost_bets)                                                       AS lost_bets,
    SUM(ta.cashout_bets)                                                    AS cashout_bets,
    ROUND(DIV0(SUM(ta.cashout_bets), SUM(ta.bet_count)) * 100, 2)         AS cashout_rate_pct,

    -- Odds profile (stake-weighted avg across cohort)
    ROUND(SUM(pq.avg_odds_weighted * pq.total_to_eur)
        / NULLIF(SUM(pq.total_to_eur), 0), 4)                              AS avg_odds_weighted,
    SUM(pq.bets_odds_under_1_5)                                             AS bets_odds_under_1_5,
    SUM(pq.bets_odds_1_5_to_3)                                             AS bets_odds_1_5_to_3,
    SUM(pq.bets_odds_over_3)                                               AS bets_odds_over_3,
    ROUND(DIV0(SUM(pq.bets_odds_under_1_5), SUM(ta.bet_count)) * 100, 2) AS pct_odds_under_1_5,
    ROUND(DIV0(SUM(pq.bets_odds_1_5_to_3),  SUM(ta.bet_count)) * 100, 2) AS pct_odds_1_5_to_3,
    ROUND(DIV0(SUM(pq.bets_odds_over_3),    SUM(ta.bet_count)) * 100, 2) AS pct_odds_over_3,

    -- GGR
    SUM(pq.total_net_stake_eur)                                             AS total_net_stake_eur,
    ROUND(SUM(pq.total_to_eur) - SUM(pq.total_net_stake_eur), 2)          AS ggr_proxy,
    ROUND(DIV0(SUM(pq.total_to_eur) - SUM(pq.total_net_stake_eur),
               SUM(pq.total_to_eur)) * 100, 2)                             AS margin_pct

FROM player_quarter pq
LEFT JOIN ticket_agg ta
    ON ta.player_id = pq.player_id
    AND ta.quarter  = pq.quarter

GROUP BY pq.quarter
ORDER BY pq.quarter;
