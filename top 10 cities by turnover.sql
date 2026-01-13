WITH actives AS (
    SELECT
        player_id,
        SUM(bet_total) AS turnover
    FROM PROD.DM_PLAYER.F_PLAYER_PRODUCT_PERFORMANCE_DAILY
    WHERE business_domain_id = 3
    AND DATE_TRUNC('year', reporting_date) = '2025-01-01'
    AND player_id NOT IN (SELECT DISTINCT player_id FROM PROD.DWH.D_PLAYER WHERE is_test_account = 1)
    GROUP BY ALL
    HAVING SUM(bet_total) > 0
),

player_details AS (
    SELECT
        p.player_id,
        p.region,
        p.gender
    FROM PROD.DWH.D_PLAYER p
    INNER JOIN actives a ON p.player_id = a.player_id
    WHERE p.business_domain_id = 3
),

player_with_turnover AS (
    SELECT
        pd.player_id,
        pd.region,
        pd.gender,
        a.turnover
    FROM player_details pd
    INNER JOIN actives a ON pd.player_id = a.player_id
)

-- Query 1: Percentage distribution of active players by gender
/*SELECT
    gender,
    COUNT(DISTINCT player_id) AS number_of_players,
    ROUND(COUNT(DISTINCT player_id) * 100.0 / SUM(COUNT(DISTINCT player_id)) OVER (), 2) AS percentage
FROM player_details
GROUP BY gender
ORDER BY number_of_players DESC;
*/
-- Query 2: Top 10 cities by number of active players
SELECT
    region,
    COUNT(DISTINCT player_id) AS number_of_players,
    ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT player_id) DESC) AS rank
FROM player_details
WHERE region IS NOT NULL
GROUP BY 1
ORDER BY number_of_players DESC
LIMIT 12;

-- Query 3: Top 10 cities by total amount wagered
SELECT
    region,
    SUM(turnover) AS total_wagered,
    ROW_NUMBER() OVER (ORDER BY SUM(turnover) DESC) AS rank
FROM player_with_turnover
WHERE region IS NOT NULL
GROUP BY 1
ORDER BY total_wagered DESC
LIMIT 12;

