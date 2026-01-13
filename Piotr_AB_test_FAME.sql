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

