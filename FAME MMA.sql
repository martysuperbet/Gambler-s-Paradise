--1.4K users it is one of first 3 bets ever, but 110 for freshly registered 
WITH player_first_tickets AS (
    SELECT
        player_id,
        ticket_code,
        MIN(accepted_dt) as ticket_accepted_dt
    FROM PROD.DWH.F_TICKET_SELECTION
    WHERE business_domain_id = 3
    and accepted_dt::date >= '2025-10-30' --first bet 
    GROUP BY all
    QUALIFY DENSE_RANK() OVER (PARTITION BY player_id ORDER BY MIN(accepted_dt)) <= 3
),

--% how many tickets have just fame in it (no other sport) -- to do
fame as (
SELECT
    b.player_id,
    b.ticket_code,

  FROM PROD.DWH.F_TICKET_SELECTION b
  WHERE business_domain_id = 3 
    AND tournament_id = '93628'
    group by all
),
group_1b as (
    SELECT DISTINCT
        tt.player_id,
        --tt.ticket_code
    FROM player_first_tickets tt 
    join fame
        on tt.ticket_code = fame.ticket_code
    left join (select player_id, registration_dt from PROD.DWH.D_PLAYER where business_domain_id = 3 and is_test_account <> 1) pp
        on tt.player_id = pp.player_id
    where pp.registration_dt::date >= '2025-10-30'
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

select distinct player_id,from gg

/*
add:
rank
ftd,
total deposit,
ggr,
ngr,
RR (in weeks)
other sports
avg stake