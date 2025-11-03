--Which Polish clubs (soccer, volleyball, basketball, etc.) do players most often place bets on?

WITH event AS (
  SELECT
    tournament_id,
    sport_id,
    sport_name,
    event_id,
    tournament_name,
    competitor_one_name,
    competitor_two_name,
    category_name
  FROM PROD.DWH.D_EVENT
  where lower(category_name) like '%poland%'
),
competitor_extraction AS (
  SELECT
    b.ticket_id,
    b.ticket_code,
    b.selection_id,
    b.event_name,
    b.tournament_name,
    --tournament_id,
    b.outcome_name AS competitor_betted_for,
    e.competitor_one_name,
    e.competitor_two_name,
    e.sport_name,
  FROM  PROD.DM_SPORT.F_TICKET_SELECTION_2Y_MATERIALIZED b
  JOIN event e ON  b.event_id = e.event_id
  WHERE business_domain_id = 3 
    AND date_trunc(year,resolved_dt::date) = '2025-01-01'
    group by all
),

unnested_competitors AS (
  SELECT
    ticket_id,
    ticket_code,
    selection_id,
    sport_name,
    tournament_name,
    CASE 
      WHEN competitor_betted_for LIKE '%{$competitor1}%' THEN competitor_one_name
      ELSE NULL
    END AS competitor_name
  FROM competitor_extraction
  WHERE competitor_betted_for LIKE '%{$competitor1}%'
  
  UNION ALL
  
  SELECT
    ticket_id,
    ticket_code,
    selection_id,
    sport_name,
    tournament_name,
    CASE 
      WHEN competitor_betted_for LIKE '%{$competitor2}%' THEN competitor_two_name
      ELSE NULL
    END AS competitor_name
  FROM competitor_extraction
  WHERE competitor_betted_for LIKE '%{$competitor2}%'
)
SELECT
    sport_name,
  competitor_name,
  COUNT(DISTINCT ticket_id) AS num_tickets,
  COUNT(selection_id) AS num_selections
FROM unnested_competitors
WHERE competitor_name IS NOT NULL
GROUP BY all
ORDER BY num_tickets DESC