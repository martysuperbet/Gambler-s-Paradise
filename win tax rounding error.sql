-- rounding discrapencies on win tax amount (round error)
-- list of tickets with this issue

SELECT *
    --TRANSACTION_TYPE 
    
FROM PROD.DWH.F_TICKET
WHERE BUSINESS_DOMAIN_ID = 3
AND RESOLVED_DT::DATE >= '2025-09-30'
and player_id = '39e1a884-193c-5644-b0a1-554c99369276'


with base as (
SELECT 
date_trunc(month,resolved_dt::date) as month,
resolved_dt,
ticket_code,
player_id,
initial_payout_total_amt,
--initial_payout_net_amt,
--ACTUAL_PAYOUT_TOTAL_AMT,
ACTUAL_PAYOUT_NET_AMT,
--user_agent,
--IS_CASHOUT_STAKE_BACK,
--CASHOUT_MARGIN,
--SELECTION_COUNT_CASHOUT
from PROD.DWH.F_TICKET
WHERE BUSINESS_DOMAIN_ID = 3
and date_trunc(year,resolved_dt::date) = '2025-01-01'
and initial_payout_total_amt = '2280.00'
and actual_payout_net_amt = '2052.00'
and ticket_status = 'WIN'
AND SELECTION_COUNT_CASHOUT = 0
ORDER BY resolved_dt DESC
--and ticket_code = '8954-RRPF98'
)

/*select
date_trunc('month',resolved_dt) as month,
count(distinct ticket_code) as no_tickets
from base
group by all
order by month desc*/
