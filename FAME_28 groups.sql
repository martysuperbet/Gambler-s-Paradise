WITH groupC as (
with activity AS (
    SELECT 
        PLAYER_ID,              
        MAX(reporting_date) AS last_active_day,     
        DATEDIFF('day', CURRENT_DATE()-1, MAX(reporting_date)) * -1 AS last_activity_was_days_ago 
    FROM PROD.DM_PLAYER.F_PLAYER_PRODUCT_PERFORMANCE_DAILY    
    WHERE reporting_date <= '2025-10-29'  -- Day before FAME event became available
        AND business_line_id = 1 
        AND business_domain_id = 3
    GROUP BY 1
),
category AS (
    SELECT
        *,
        CASE 
            WHEN last_activity_was_days_ago > 31 AND last_activity_was_days_ago <= 90 THEN 'Churn'
            WHEN last_activity_was_days_ago > 90 /*AND last_activity_was_days_ago <= 364*/ THEN 'Dormant'
            WHEN last_activity_was_days_ago IS NULL THEN 'No activity'
            WHEN last_activity_was_days_ago <= 30 THEN 'Active last 30 days'
            ELSE 'Not classified' 
        END AS Activity
    FROM activity
),
fame AS (
    SELECT
        player_id,
        MIN(accepted_dt) AS first_fame_bet_dt,
        COUNT(DISTINCT ticket_code) AS fame_bets_count
    FROM PROD.DWH.F_TICKET_SELECTION
    WHERE business_domain_id = 3 
        AND tournament_id = '93628'
        AND accepted_dt::DATE >= '2025-10-30'
        and ticket_status <> 'CANCELLED'
    GROUP BY 1
),
first_bet_after_inactivity AS (
    SELECT
        player_id,
        MIN(accepted_dt) AS first_bet_after_inactivity_dt,
        MIN(CASE WHEN tournament_id = '93628' THEN accepted_dt END) AS first_fame_bet_check
    FROM PROD.DWH.F_TICKET_SELECTION
    WHERE business_domain_id = 3
        AND accepted_dt::DATE >= '2025-10-30'
        and ticket_status <> 'CANCELLED'
    GROUP BY 1
)
SELECT
    'group C' as grupa,
    c.player_id,
    c.last_active_day,
    c.last_activity_was_days_ago,
    c.Activity,
    f.first_fame_bet_dt,
    f.fame_bets_count,
    fb.first_bet_after_inactivity_dt

FROM category c
INNER JOIN fame f ON c.player_id = f.player_id
INNER JOIN first_bet_after_inactivity fb ON c.player_id = fb.player_id
WHERE c.Activity IN ('Churn', 'Dormant')
    AND fb.first_bet_after_inactivity_dt = fb.first_fame_bet_check  -- First bet after 30 Oct was FAME
ORDER BY c.last_activity_was_days_ago DESC
),
-------
groupA as (
WITH player_first_tickets AS (
SELECT
    player_id,
    ticket_code,
    MIN(accepted_dt) as ticket_accepted_dt,
    DENSE_RANK() OVER (PARTITION BY player_id ORDER BY MIN(accepted_dt)) as bet_rank
FROM PROD.DWH.F_TICKET_SELECTION
WHERE business_domain_id = 3
and ticket_status <> 'CANCELLED'
    --AND accepted_dt::date >= '2025-10-30' --first bet 
GROUP BY all
QUALIFY bet_rank <= 10
    /*SELECT
        player_id,
        ticket_code,
        MIN(accepted_dt) as ticket_accepted_dt
    FROM PROD.DWH.F_TICKET_SELECTION
    WHERE business_domain_id = 3
    and accepted_dt::date >= '2025-10-30' --first bet 
    GROUP BY all
    QUALIFY DENSE_RANK() OVER (PARTITION BY player_id ORDER BY MIN(accepted_dt)) <= 3*/
),

--% how many tickets have just fame in it (no other sport) -- to do
fame as (
SELECT
    b.player_id,
    b.ticket_code,

  FROM PROD.DWH.F_TICKET_SELECTION b
  WHERE business_domain_id = 3 
    AND tournament_id = '93628'
    and ticket_status <> 'CANCELLED'
    group by all
),
group_1b as (
    SELECT DISTINCT
        tt.player_id,
        min(tt.bet_rank) as bet_rank,
        --tt.ticket_code
    FROM player_first_tickets tt 
    join fame
        on tt.ticket_code = fame.ticket_code
    left join (select player_id, registration_dt, is_test_account from PROD.DWH.D_PLAYER where business_domain_id = 3) pp
        on tt.player_id = pp.player_id
    where pp.registration_dt::date >= '2025-10-15'
        and pp.is_test_account <> 1
    group by all
),

group_1a as ( --registered with code
    select
        player_id,
        coalesce(marketing_coupon_code,coupon_code) as coupon_code1,
    from PROD.DWH.D_PLAYER_MARKETING_PROPERTY
    WHERE business_domain_id = 3
        and current_timestamp() between valid_from_dt and valid_to_dt
        and upper(coupon_code1) like '%FAME%'
    group by all
    ),

gg as (
select distinct player_id from group_1a
union all
select player_id from group_1b
)

--matrix as (
select 
    gg.player_id,
    cc.coupon_code1 as coupon_code,
    bb.bet_rank,
    'group A' as grupa,
from gg
left join group_1a as cc
    on gg.player_id = cc.player_id
left join group_1B as bb
    on gg.player_id = bb.player_id
group by all
),

fin as (
select distinct 
grupa, player_id from groupC
union all
select distinct 
grupa, player_id from groupA
),

all_players as (
    -- First, get all Group A and C players
    SELECT DISTINCT 
        player_id,
        grupa
    FROM fin
    
    UNION
    
    -- Then add Group B players (who bet on FAME but aren't in A or C)
    SELECT DISTINCT 
        FF.player_id,
        'group B' as grupa
    FROM PROD.DWH.F_TICKET_SELECTION FF
    LEFT JOIN fin ON FF.player_id = fin.player_id
    WHERE FF.business_domain_id = 3 
        AND FF.tournament_id = '93628'
        AND FF.accepted_dt::DATE >= '2025-10-30'
        AND FF.ticket_status <> 'CANCELLED'
        AND fin.player_id IS NULL  -- Only players NOT in Group A or C
)

SELECT
    all_players.player_id,
    all_players.grupa,
    COALESCE(groupA.coupon_code, 'N/A') as coupon_code,
    groupA.bet_rank as bet_rank,
    groupC.last_active_day,
    groupC.last_activity_was_days_ago,
    groupC.Activity,
    groupC.first_fame_bet_dt,
    groupC.first_bet_after_inactivity_dt,
    COUNT(DISTINCT FF.ticket_code) as fame_bets_count
    
FROM all_players
LEFT JOIN PROD.DWH.F_TICKET_SELECTION FF
    ON all_players.player_id = FF.player_id
    AND FF.business_domain_id = 3 
    AND FF.tournament_id = '93628'
    AND FF.accepted_dt::DATE >= '2025-10-30'
    AND FF.ticket_status <> 'CANCELLED'
LEFT JOIN groupA ON all_players.player_id = groupA.player_id
LEFT JOIN groupC ON all_players.player_id = groupC.player_id
LEFT JOIN (SELECT player_id, is_test_account FROM PROD.DWH.D_PLAYER WHERE business_domain_id = 3) xx
    ON all_players.player_id = xx.player_id
WHERE COALESCE(xx.is_test_account, 0) <> 1
GROUP BY ALL
