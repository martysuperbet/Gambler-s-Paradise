with comms as (
select 
    campaign_id,
    campaign_name,
    campaign_send_date as reporting_date,
    case when message_type = 'PUSH_IOS' then 'PUSH'
        when message_type = 'PUSH_ANDROID' then 'PUSH'
        else message_type
    end as message_type,
    user_id as player_id,
    CASE
    -- 23rd
    WHEN campaign_id = '128808179' THEN 'group A'
    WHEN campaign_id = '128808197' THEN 'group B'
    WHEN campaign_id = '128808217' THEN 'group C'
    -- 25th
    WHEN campaign_id = '128836576' THEN 'group A'
    WHEN campaign_id = '128836584' THEN 'group B'
    WHEN campaign_id = '128836702' THEN 'group C'
    -- 26th
    WHEN campaign_id = '128852195' THEN 'group A'
    WHEN campaign_id = '128853431' THEN 'group B'
    WHEN campaign_id = '128852203' THEN 'group C'
    -- 28th
    WHEN campaign_id = '128874702' THEN 'group A'
    WHEN campaign_id = '128874832' THEN 'group B'
    WHEN campaign_id = '128874886' THEN 'group C'
    -- 29th
    WHEN campaign_id = '128892322' THEN 'group A'
    WHEN campaign_id = '128892377' THEN 'group B'
    WHEN campaign_id = '128892414' THEN 'group C'
    -- 30th 
    WHEN campaign_id = '128905437' THEN 'group A'
    WHEN campaign_id = '128905475' THEN 'group B'
    end as player_group,
    count (distinct case when interaction_type = 'sent' then user_id else null end) as sends,
    count(distinct case when interaction_type = 'bounce' then user_id else null end) as bounces,
    count(distinct case when interaction_type = 'click' then user_id else null end) as distinct_clicks,
    count(distinct case when interaction_type = 'open' then user_id else null end) as distinct_opens

from ods_xp.campaign 
where campaign_id in 
(/*23*/'128808179', '128808197', '128808217', /*24*/ '128823464', /*25*/ '128836576', '128836584', '128836702',  /*26*/ '128852195', '128853431','128852203', /*27*/ '128871810', /*28*/ '128874702', '128874832','128874886', /*29*/ '128892322', '128892377', '128892414', /*30*/ '128905437', '128905475')
group by all
),

player_matrix as (
SELECT 
    pm.player_id,
    pm.player_group,
    pm.reporting_date,
    --message_type,
from comms pm
where reporting_date in ('2025-11-28')
group by all
),


/*
player_matrix as (
select user_id as player_id from PROD.BI.GROUPCFAME
),
*/

sg_hunch_plays as (
/*SELECT 
RECORD_CONTENT:domain::varchar AS HUNCH_DOMAIN,
'supergame' AS GAME_TYPE,
REGEXP_SUBSTR(
        RECORD_METADATA:s3_file::STRING, 
        'hunch/output/(.*?)/[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}', 
        1, 
        1, 
        'e'
    ) AS GAME_NAME,
RECORD_CONTENT:user_id::varchar AS USER_ID_HASH,
RECORD_CONTENT:user_uuid::varchar AS USER_UUID_HASH,
to_varchar(
	try_decrypt_raw(
		to_binary(RECORD_CONTENT:user_uuid::varchar , 'BASE64'), 
		to_binary(CASE HUNCH_DOMAIN WHEN 'SB_RO' THEN '1XmSFD1AkiYRYH9vygJURj6zenK55s8m' WHEN 'SB_PL' THEN 'amOLaCmk7dWusoLLjKjlkE01nds0vU4X' END, 'UTF-8') , 
		to_binary(CASE HUNCH_DOMAIN WHEN 'SB_RO' THEN 'lKwSJ3frJMCf88Fe' WHEN 'SB_PL' THEN 'BttY8Aq6BDuSEmhd' END, 'UTF-8' ), 
		null, 'AES-CBC'
	)  
	,'utf-8'
) AS PLAYER_EXTERNAL_CODE_DEC,
to_varchar(
	try_decrypt_raw(
		to_binary(RECORD_CONTENT:user_uuid::varchar , 'BASE64'), 
		to_binary(CASE HUNCH_DOMAIN WHEN 'SB_RO' THEN '1XmSFD1AkiYRYH9vygJURj6zenK55s8m' WHEN 'SB_PL' THEN 'amOLaCmk7dWusoLLjKjlkE01nds0vU4X' END, 'UTF-8') , 
		to_binary(CASE HUNCH_DOMAIN WHEN 'SB_RO' THEN 'lKwSJ3frJMCf88Fe' WHEN 'SB_PL' THEN 'BttY8Aq6BDuSEmhd' END, 'UTF-8' ), 
		null, 'AES-CBC'
	)  
	,'utf-8'
) AS PLAYER_ID_DEC,
dp.BUSINESS_DOMAIN_CODE ,
dp.BUSINESS_MARKET_CODE ,
dp.PLAYER_ID ,
CONVERT_TIMEZONE('UTC', dbm.REPORTING_TIMEZONE  , RECORD_CONTENT:submission_time::timestamp) AS SUBMISSION_TIME, --local
DATE(CONVERT_TIMEZONE('UTC', dbm.REPORTING_TIMEZONE  , RECORD_CONTENT:submission_time::timestamp)) AS SUBMISSION_DATE, --local
RECORD_CONTENT:round_number::int AS ROUND_NUMBER,
RECORD_METADATA:s3_file_last_modified::timestamp AS INS_TIMESTAMP
FROM RAW_GAMING.F2P_HUNCH_SUPERGAMES sp
JOIN DWH.D_PLAYER dp 
    ON dp.PLAYER_ID = PLAYER_ID_DEC 
    AND dp.IS_TEST_ACCOUNT = 0
JOIN DWH.D_BUSINESS_MARKET dbm
    ON dbm.business_market_id = dp.business_market_id
WHERE 1=1
-- non-ex-sportcaller countries
AND HUNCH_DOMAIN IN ('SB_RO')
-- remove test and missing data
AND USER_ID_HASH IS NOT NULL
AND RECORD_CONTENT:submission_time::timestamp >= '2025-03-20 00:00:00.000'
QUALIFY ROW_NUMBER() OVER (PARTITION BY HUNCH_DOMAIN, USER_ID_HASH, ROUND_NUMBER ORDER BY INS_TIMESTAMP DESC) = 1
*/

-- price boost 
select
    date_trunc('day', fts.accepted_dt) as reporting_date,
    fts.business_domain_name,
    fts.player_id
from prod.dm_sport.f_ticket_selection_generosity fts
where true
    and date(fts.accepted_dt) between '2025-01-01' and current_date - 1
    and fts.was_boosted_odd_price is not null
    and fts.business_line_code = 'ONLINE'
    and fts.selection_count is not null
    and business_domain_id = 3
group by all

/*
--supergame
SELECT 
    RECORD_CONTENT:sb_user_id::VARCHAR AS sb_user_id, 
    RECORD_CONTENT:sb_user_uuid::VARCHAR AS player_id,
    RECORD_CONTENT:submission_time::DATE as reporting_date,
    --RECORD_CONTENT:round_name::DATE as game_type
    FROM PROD.RAW_GAMING.F2P_HUNCH_SUPERGAMES
    WHERE RECORD_CONTENT:submission_time::DATE >= '2025-07-20'
        AND RECORD_CONTENT:domain = 'SB_PL'
*/
/*
--superspin
select 
player_id,
--max(submission_ts::date) as day
date_trunc(day,submission_ts::date) as reporting_date
from PROD.DM_PLAYER.F_PLAYER_ENGAGE
where business_market_code = 'SB_PL'
and player_id is not null
and game_code = 'superspin'
group by all
*/
),

sg_hunch_final AS (
SELECT
    sg.PLAYER_ID,
    pp.username,
    date_trunc(day,reporting_date) as reporting_date,

FROM sg_hunch_plays sg
left join (select player_id, username, registration_dt from DWH.D_PLAYER where business_domain_id = 3 )pp
on sg.PLAYER_ID = pp.PLAYER_ID
--where SUBMISSION_DATE::date = '2024-11-24'
)

SELECT 
    pm.player_group,
    pm.reporting_date,
    --pm.message_type,
    COUNT(DISTINCT pm.player_id) AS total_players_in_group,
    COUNT(DISTINCT sg.PLAYER_ID) AS players_with_game_played,
    COUNT(DISTINCT CASE WHEN sg.PLAYER_ID IS NOT NULL THEN pm.player_id END) AS players_played_count,
    ROUND(players_with_game_played / total_players_in_group * 100, 2) AS play_rate_pct
FROM player_matrix pm
LEFT JOIN sg_hunch_final sg 
    ON pm.player_id = sg.PLAYER_ID
   AND sg.reporting_date = '2025-11-28'  -- Games played on or after campaign date
   --AND sg.reporting_date <= DATEADD('day', 7, '2025-11-28')--pm.reporting_date)  -- Within 7 days (adjust as needed)
    --AND sg.reporting_date >= DATEADD('day', 14, '2025-11-28')  -- Games played on or after campaign date
    --AND sg.reporting_date <= DATEADD('day', 21, '2025-11-28')  -- Within 7 days (adjust as needed)
GROUP BY all
order by player_group asc

----------------------------------------------
--click rate 
--bonus assigned/used

with comms as (
select 
    campaign_id,
    campaign_name,
    campaign_send_date as reporting_date,
    case when message_type = 'PUSH_IOS' then 'PUSH'
        when message_type = 'PUSH_ANDROID' then 'PUSH'
        else message_type
    end as message_type,
    user_id as player_id,
    CASE
    -- 23rd
    WHEN campaign_id = '128808179' THEN 'group A'
    WHEN campaign_id = '128808197' THEN 'group B'
    WHEN campaign_id = '128808217' THEN 'group C'
    -- 25th
    WHEN campaign_id = '128836576' THEN 'group A'
    WHEN campaign_id = '128836584' THEN 'group B'
    WHEN campaign_id = '128836702' THEN 'group C'
    -- 26th
    WHEN campaign_id = '128852195' THEN 'group A'
    WHEN campaign_id = '128853431' THEN 'group B'
    WHEN campaign_id = '128852203' THEN 'group C'
    -- 28th
    WHEN campaign_id = '128874702' THEN 'group A'
    WHEN campaign_id = '128874832' THEN 'group B'
    WHEN campaign_id = '128874886' THEN 'group C'
    -- 29th
    WHEN campaign_id = '128892322' THEN 'group A'
    WHEN campaign_id = '128892377' THEN 'group B'
    WHEN campaign_id = '128892414' THEN 'group C'
    -- 30th 
    WHEN campaign_id = '128905437' THEN 'group A'
    WHEN campaign_id = '128905475' THEN 'group B'
    end as player_group,
    count (distinct case when interaction_type = 'sent' then user_id else null end) as sends,
    count(distinct case when interaction_type = 'bounce' then user_id else null end) as bounces,
    count(distinct case when interaction_type = 'click' then user_id else null end) as distinct_clicks,
    count(distinct case when interaction_type = 'open' then user_id else null end) as distinct_opens

from ods_xp.campaign 
where campaign_id in 
(/*23*/'128808179', '128808197', '128808217', /*24*/ '128823464', /*25*/ '128836576', '128836584', '128836702',  /*26*/ '128852195', '128853431','128852203', /*27*/ '128871810', /*28*/ '128874702', '128874832','128874886', /*29*/ '128892322', '128892377', '128892414', /*30*/ '128905437', '128905475')
group by all
),

comms_grouped as (
select
    campaign_id,
    campaign_name,
    reporting_date::date as reporting_date,
    message_type,
    player_group,
    count(distinct player_id) as players,
    SUM(distinct_clicks) as click,
    SUM(distinct_clicks)/sum(sends) as click_rate,
    SUM(bounces)/sum(sends) as bounce_rate
from comms
group by all
order by reporting_date desc
),

player_matrix as (
SELECT 
    pm.player_id,
    pm.player_group,
    pm.reporting_date,
    message_type,
from comms pm
group by all
),

bonuses as (
select
    pp.player_id,
    player_group,
    message_type,
    pp.bonus_code,
    pp.bonus_type,   
    pp.bonus_name,
    pp.bonus_status,
    pp.bonus_rewarded_dt,
    pp.bonus_rewarded_amount,
    pp.bonus_redeemed_dt,
    pp.bonus_redeemed_amount,
    bonus_cost_amount
from PROD.DWH.D_PLAYER_BONUS pp
left join player_matrix mm
    on pp.player_id = mm.player_id
    and pp.bonus_rewarded_dt::date = mm.reporting_date
where business_domain_id = 3 
and campaign_id in (/*23*/'6ea3d836-860a-490e-89e2-e3b8c91e1d3f','f1dfb401-7cf5-40cf-90b7-e710bee42f3f','69a40d40-dca9-4b55-ac43-082a51d09347',/*25*/ 'f681fc92-95b7-4206-91e3-f1b973d3aa71', 'aa62dad5-36ed-4eec-bc7a-f6d2329cc12b', /*26*/ 'dbdbc3f6-ea7d-4ef4-8bc0-39a65e2517bb','4d9b40aa-3709-4a28-9b65-f7535074cdd3', /*28*/ '66c20449-558e-40e8-8fcf-f0d5f7b8f2f2', '03dac2d1-45c4-4620-8737-cda2b1e25c74','34fa3481-fe51-476a-83a5-025e89177822',/*29*/'818e00bb-ab05-4d16-9f9f-67ad953ebcf1','2b4308e2-db1d-4fe8-a702-f4f1a933e484', 'a421fd7f-493d-45c1-8bb0-dfc381e564c0')
order by bonus_redeemed_amount desc
--24 & 27 sprawdzic superspin
--30 sprawdzic SuperGame
),

bonus_grouped as (
select
--bonus_code,
bonus_name,
--message_type,
--player_group,
--bonus_rewarded_amount,
bonus_rewarded_dt::date,
count(player_id) as bonusses_assigned,
count(case when bonus_status = 'COMPLETED' then bonus_rewarded_dt end) as redeems,
sum(bonus_cost_amount) as bonus_cost,
from bonuses 
group by all
order by bonus_rewarded_dt::date asc 
)

select * from bonus_grouped

--24 & 27 sprawdzic superspin
--30 sprawdzic SuperGame
--sprawdzić czy freebety się dobrze zapisują (wszystkie na 0) DONE
