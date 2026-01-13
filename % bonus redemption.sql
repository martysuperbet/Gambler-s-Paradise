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
        AND bonus_name IN ('WB_Cashback_Bonus', 'Welcome Bonus Cashback bonus v6 03.2024')
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

redemption_categories AS (
    SELECT
        player_id,
        bonus_id,
        bonus_name,
        INITIAL_BONUS_AMOUNT,
        total_redeemed,
        redemption_percentage,
        days_to_last_redemption,
        CASE 
            WHEN redemption_percentage >= 100 THEN '100% (Full Redemption)'
            WHEN redemption_percentage >= 75 THEN '75-99% Redemption'
            WHEN redemption_percentage >= 50 THEN '50-74% Redemption'
            WHEN redemption_percentage >= 25 THEN '25-49% Redemption'
            ELSE '0-24% Redemption'
        END AS redemption_category
    FROM player_bonus_summary
)

--select * from 
--redemption_categories
--order by redemption_percentage desc

-- Final aggregated results
SELECT
    redemption_category,
    COUNT(DISTINCT player_id) AS number_of_players,
    ROUND(COUNT(DISTINCT player_id) * 100.0 / SUM(COUNT(DISTINCT player_id)) OVER (), 2) AS percentage_of_players,
    ROUND(AVG(redemption_percentage), 2) AS avg_redemption_pct,
    ROUND(AVG(days_to_last_redemption), 1) AS avg_days_to_redeem,
    ROUND(MEDIAN(days_to_last_redemption), 1) AS median_days_to_redeem,
    ROUND(AVG(INITIAL_BONUS_AMOUNT), 2) AS avg_bonus_amount,
    ROUND(AVG(total_redeemed), 2) AS avg_amount_redeemed
FROM redemption_categories
GROUP BY redemption_category
ORDER BY 
    CASE redemption_category
        WHEN '100% (Full Redemption)' THEN 1
        WHEN '75-99% Redemption' THEN 2
        WHEN '50-74% Redemption' THEN 3
        WHEN '25-49% Redemption' THEN 4
        ELSE 5
    END;
