with marketing as (
    select
        player_id,
        HAS_MARKETING_CONSENT,
        HAS_MARKETING_PHONE_CONSENT,
    from PROD.DWH.D_PLAYER_MARKETING_PROPERTY
    WHERE business_domain_id = 3
        and current_timestamp() between valid_from_dt and valid_to_dt
    group by all
),

phone as (
SELECT
PLAYER_ID,
MOBILE_PHONE,
FROM PROD.DWH.D_PLAYER_PROFILE
WHERE business_domain_id = 3
        and current_timestamp() between valid_from_dt and valid_to_dt
),

player as (
SELECT
PLAYER_ID,
IS_MOBILE_VERIFIED
FROM PROD.DWH.D_PLAYER
WHERE business_domain_id = 3
),

 turnover_mar AS (
    SELECT 
        player_id,
        SUM(bet_total) AS turnover_march
    FROM PROD.DM_PLAYER.F_PLAYER_PRODUCT_PERFORMANCE_DAILY
    WHERE business_domain_id = 3
        AND bet_total > 0
        AND DATE_TRUNC('month', reporting_date) = '2025-03-01'
    GROUP BY player_id
),

turnover_apr AS (
    SELECT DISTINCT
        player_id
    FROM PROD.DM_PLAYER.F_PLAYER_PRODUCT_PERFORMANCE_DAILY
    WHERE business_domain_id = 3
        AND bet_total > 0
        AND DATE_TRUNC('month', reporting_date) = '2025-04-01'
),

top_3_pct AS (
    SELECT
        player_id,
        turnover_march,
        PERCENT_RANK() OVER (ORDER BY turnover_march DESC) AS pct_rank
    FROM turnover_mar
)

SELECT
    t.player_id,
    t.turnover_march,
    HAS_MARKETING_CONSENT,
    HAS_MARKETING_PHONE_CONSENT,
    IS_MOBILE_VERIFIED,
    MOBILE_PHONE,
    --t.pct_rank
FROM top_3_pct t
LEFT JOIN turnover_apr apr 
    ON t.player_id = apr.player_id
left join marketing
    on t.player_id = marketing.player_id
left join phone
    on t.player_id = phone.player_id
left join player
    on t.player_id = player.player_id

WHERE apr.player_id IS NULL  -- not active in April at all
ORDER BY t.turnover_march DESC
