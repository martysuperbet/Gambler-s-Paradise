-- social inspired tickets per specific event
with base as (
select
reference_ticket_code,
even,
count(distinct bb.ticket_code) as ct
from PROD.DM_SOCIAL.F_INSPIRED_TICKET bb
left join (
select
    ticket_code,
    'fame' as even
from PROD.DWH.F_TICKET_SELECTION
where tournament_id = '93628'
group by all 
) ss
on reference_ticket_code = ss.ticket_code
where business_domain_id = 3 
and even is not null
group by all
order by 2 desc
)
select
count(distinct reference_ticket_code) as original,
sum(ct) as copies
from base 
