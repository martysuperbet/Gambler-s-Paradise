WITH bonus_transactions AS (
    -- Get all bonus transactions with redemption details
    SELECT
        f.player_id,
        f.bonus_code,
        f.bonus_id,
        d.bonus_name,
        f.TRANSACTION_DT_LOCAL,
        f.WALLET_TRANSACTION_ID,
        f.BONUS_REWARDED_DT,
        f.INITIAL_BONUS_AMOUNT,
        f.BONUS_MONEY_AMOUNT,
        f.REMAINING_BONUS_BALANCE
    FROM PROD.DWH.F_PLAYER_BONUS f
    JOIN (
        SELECT
            bonus_id,
            bonus_code,
            bonus_name
        FROM PROD.DWH.D_BONUS
        WHERE business_domain_id = 3
        AND bonus_name IN (/*betler*/ 'WB_Deposit_Freebet', /*icore*/'Welcome Freebet _V5.1_11.2023','Welcome Freebet _V5.1_Manual_11.2023')
        GROUP BY ALL
    ) d ON f.bonus_id = d.bonus_id
    where f.BONUS_REWARDED_DT <> '0001-01-01 00:00:00.000'
    and event_type = 'approved'
    and player_id is not null
    and player_id not in (select distinct player_id from PROD.DWH.D_PLAYER where business_domain_id = 3 and is_test_account = 1)
),
--need to clean for 0001-01-01 00:00:00.000
-- some players hunderds of %


player_bonus_summary AS (
    -- Aggregate per player and bonus instance
    SELECT
        player_id,
        bonus_id,
        bonus_code,
        bonus_name,
        max(INITIAL_BONUS_AMOUNT) as INITIAL_BONUS_AMOUNT,
        SUM(BONUS_MONEY_AMOUNT) AS total_redeemed,
        MIN(BONUS_REWARDED_DT) AS bonus_rewarded_dt,
        MAX(TRANSACTION_DT_LOCAL) AS last_redemption_dt,
        MIN(REMAINING_BONUS_BALANCE) AS final_remaining_balance, 
        -- Calculate redemption percentage
        CASE 
            WHEN INITIAL_BONUS_AMOUNT > 0 
            THEN (SUM(BONUS_MONEY_AMOUNT) / max(INITIAL_BONUS_AMOUNT)) * 100 
            ELSE 0 
        END AS redemption_percentage,
        -- Calculate days to full redemption
        DATEDIFF('day', MIN(BONUS_REWARDED_DT), MAX(TRANSACTION_DT_LOCAL)) AS days_to_last_redemption
    FROM bonus_transactions
    WHERE INITIAL_BONUS_AMOUNT > 0
    GROUP BY player_id, bonus_id, bonus_code, bonus_name, INITIAL_BONUS_AMOUNT
),

target_players AS (
    -- Filter for 75-99% redeemers
    SELECT DISTINCT
        player_id,
        bonus_rewarded_dt,
        last_redemption_dt,
        redemption_percentage,
        DATE_TRUNC('month', last_redemption_dt) AS last_redemption_month
    FROM player_bonus_summary
    WHERE redemption_percentage >= 100
),

tier_at_redemption AS (
    -- Get value tier during the month of last redemption
    SELECT 
        tp.player_id,
        tp.last_redemption_dt,
        tp.last_redemption_month,
        tp.redemption_percentage,
        vt.VALUE_TIER_NAME_OFFICIAL AS tier_at_redemption,
        vt.REPORTING_DATE_FROM AS redemption_month_start
    FROM target_players tp
    LEFT JOIN DM_PLAYER.F_PLAYER_VALUE_TIER_MONTHLY vt
        ON tp.player_id = vt.player_id
        AND DATE_TRUNC('month', tp.last_redemption_dt) = DATE_TRUNC('month', vt.REPORTING_DATE_FROM)
        AND vt.business_domain_id = 3
),

tier_next_month AS (
    -- Get value tier in the month after last redemption
    SELECT 
        tar.player_id,
        tar.last_redemption_dt,
        tar.redemption_percentage,
        tar.tier_at_redemption,
        vt.VALUE_TIER_NAME_OFFICIAL AS tier_next_month,
        vt.REPORTING_DATE_FROM AS next_month_start
    FROM tier_at_redemption tar
    LEFT JOIN DM_PLAYER.F_PLAYER_VALUE_TIER_MONTHLY vt
        ON tar.player_id = vt.player_id
        AND DATE_TRUNC('month', DATEADD('month', 1, tar.last_redemption_dt)) = DATE_TRUNC('month', vt.REPORTING_DATE_FROM)
        AND vt.business_domain_id = 3
),

tier_changes AS (
    SELECT
        player_id,
        last_redemption_dt,
        redemption_percentage,
        tier_at_redemption,
        tier_next_month,
        CASE
            WHEN tier_at_redemption IS NULL THEN 'No Tier at Redemption'
            WHEN tier_next_month IS NULL THEN 'No Tier Next Month (Churned)'
            WHEN tier_at_redemption = tier_next_month THEN 'No Change'
            ELSE 'Tier Changed'
        END AS change_status,
        CONCAT(COALESCE(tier_at_redemption, 'Unknown'), ' → ', COALESCE(tier_next_month, 'Churned/Unknown')) AS tier_transition
    FROM tier_next_month
)

-- Detailed breakdown: Only players with tier changes
SELECT
    change_status,
    tier_at_redemption,
    tier_next_month,
    tier_transition,
    COUNT(DISTINCT player_id) AS number_of_players,
    ROUND(COUNT(DISTINCT player_id) * 100.0 / SUM(COUNT(DISTINCT player_id)) OVER (), 2) AS percentage_of_players,
    ROUND(AVG(redemption_percentage), 2) AS avg_redemption_pct
FROM tier_changes
--WHERE change_status = 'Tier Changed'
GROUP BY change_status, tier_at_redemption, tier_next_month, tier_transition
ORDER BY number_of_players DESC
