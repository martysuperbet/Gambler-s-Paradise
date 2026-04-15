WITH marketing AS (
    SELECT
        player_id,
        HAS_MARKETING_CONSENT,
        HAS_MARKETING_PHONE_CONSENT
    FROM PROD.DWH.D_PLAYER_MARKETING_PROPERTY
    WHERE business_domain_id = 3
        AND current_timestamp() BETWEEN valid_from_dt AND valid_to_dt
    group by all
),

phone AS (
    SELECT
        player_id,
        MOBILE_PHONE
    FROM PROD.DWH.D_PLAYER_PROFILE
    WHERE business_domain_id = 3
        AND current_timestamp() BETWEEN valid_from_dt AND valid_to_dt
    group by all
),

player AS (
    SELECT
        player_id,
        IS_MOBILE_VERIFIED,
        email,
        value_tier_name,
        is_test_account,
        is_valid,
    FROM PROD.DWH.D_PLAYER
    WHERE business_domain_id = 3
    and is_active = 1 
    group by all
    
),

activity AS (
    SELECT
        dd.player_id,
        MAX(CASE WHEN dd.bet_total > 0 THEN dd.reporting_date END)          AS last_active_date,
        SUM(dd.bet_total)                                                           AS bet_total_eur,
        SUM(dd.ggr)                                                                 AS ggr_eur,
        SUM(dd.bonus_cost)                                                          AS bonus_cost_eur,
        COUNT(DISTINCT CASE WHEN dd.bet_total > 0 THEN dd.reporting_date END)       AS active_days,
        -- Bonus % of GGR
        ROUND(DIV0(SUM(dd.bonus_cost), SUM(dd.ggr)) * 100, 2)                      AS bonus_pct_ggr
    FROM PROD.DM_PLAYER.F_PLAYER_PRODUCT_PERFORMANCE_DAILY dd
    WHERE dd.business_domain_id = 3
    GROUP BY dd.player_id
),

fin as (
SELECT
    p.player_id,
    p.value_tier_name                                                               AS segment,
    a.last_active_date,
    DATEDIFF('day', a.last_active_date, CURRENT_DATE())                             AS days_since_active,
    --a.bet_total_eur,
    a.ggr_eur,
    a.bonus_cost_eur,
    a.bonus_pct_ggr,
    a.active_days,

    -- List B extras
    ph.mobile_phone,
    p.email,
    p.IS_MOBILE_VERIFIED,
    m.HAS_MARKETING_CONSENT,
    m.HAS_MARKETING_PHONE_CONSENT

FROM player p
INNER JOIN activity a
    ON a.player_id = p.player_id
LEFT JOIN phone ph
    ON ph.player_id = p.player_id
LEFT JOIN marketing m
    ON m.player_id = p.player_id

WHERE
    -- Last active 30+ days ago
    DATEDIFF('day', a.last_active_date, CURRENT_DATE()) > 30
    and is_valid = 1
    and is_test_account <> 1

    -- VLV and No Value: apply bonus % GGR filter
    AND (
        p.value_tier_name NOT IN ('Very Low Value', 'No Value')
        OR a.bonus_pct_ggr < 75
    )

ORDER BY a.last_active_date DESC
)

SELECT
    *,
    CASE
        WHEN MOD(ABS(HASH(player_id)), 100) < 70 THEN 'A'
        WHEN MOD(ABS(HASH(player_id)), 100) < 90 THEN 'B'
        ELSE 'C'
    END                                                     AS test_group
FROM fin
ORDER BY test_group, player_id
