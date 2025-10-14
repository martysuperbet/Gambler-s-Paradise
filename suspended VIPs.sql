--suspended players with status VIP in 2025

with status as (
    select
        player_id,
        status,
        valid_from_dt,
        valid_to_dt
    from PROD.DWH.D_PLAYER_STATUS
    where business_domain_id = 3
    and status in ('SUSPENDED','PLAYER_SUSPENDED')
    and valid_from_dt >= '2025-01-01'
),
tier as (
    select
        segmentation_month,
        player_id,
        monthly_official_segmentation
    from PROD.BI.VW_PLAYER_MONTHLY_SEGMENTATION_ALL
    where segmentation_month >= '2025-01-01'
    and business_domain_code = 'SB_PL'
    and monthly_official_segmentation = 'VIP'
    group by all
)
select distinct
    s.player_id,
    d.username,
    s.status,
    s.valid_from_dt as suspension_start_dt,
    s.valid_to_dt as suspension_end_dt,
    t.segmentation_month,
    t.monthly_official_segmentation
from status s
inner join tier t
    on s.player_id = t.player_id
    and s.valid_from_dt >= t.segmentation_month
    and s.valid_from_dt < dateadd(month, 1, t.segmentation_month)
left join (select distinct player_id, username from PROD.DWH.D_PLAYER where business_domain_id = 3 and is_test_account <> 1) d
    on s.player_id = d.player_id
order by s.player_id, s.valid_from_dt desc
