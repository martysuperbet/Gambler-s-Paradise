
with player_sports_stakes AS (
    SELECT 
        tt.player_id,
        tt.sport_name,
        SUM(tt.STAKE_TOTAL_AMT_EUR) AS total_stake
    FROM PROD.DM_SPORT.F_TICKET_SELECTION_2Y_MATERIALIZED tt
    WHERE tt.business_domain_id = 3
        AND tt.sport_name IS NOT NULL
        and ticket_status <> 'CANCELLED'
        --and accepted_dt::date >= '2025-10-15'
    GROUP BY all
)
-- Step 3: Rank sports for each player and get top 3
SELECT 
    ss.player_id,
    pp.username,
    ss.sport_name,
    total_stake,
    RANK() OVER (PARTITION BY ss.player_id ORDER BY total_stake DESC) AS stake_rank
FROM player_sports_stakes ss
left join (select player_id, username,email, is_test_account
from PROD.DWH.D_PLAYER
where business_domain_id = 3
) pp
on ss.player_id = pp.player_id
where is_test_account <> 1 
QUALIFY RANK() OVER (PARTITION BY ss.player_id ORDER BY total_stake DESC) <= 4
