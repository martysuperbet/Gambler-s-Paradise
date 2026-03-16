with selections as (
    select
        ff.player_id,
        ff.first_win_ticket_code                as ticket_code,
        mm.selection_id,
        mm.selection_coefficient,
        mm.odd_status,
        case
            when mm.selection_coefficient between 1.01 and 1.50 then 'Safe'
            when mm.selection_coefficient between 1.51 and 3.00 then 'Standard'
            when mm.selection_coefficient > 3.00                then 'High Risk'
            else 'Unknown'
        end                                     as selection_odds_bucket
    from PROD.BI.PL_FIRST_WIN ff
    left join PROD.DM_SPORT.F_TICKET_SELECTION_2Y_MATERIALIZED mm
        on ff.first_win_ticket_code = mm.ticket_code
    where mm.business_domain_id = 3
    and odd_status <> 'REFUNDED'
),

ticket_odds_classified as (
    select
        player_id,
        ticket_code,
        count(selection_id)                                         as total_selections,

        -- highest odd across ALL selections
        max(selection_coefficient)                                  as max_selection_coefficient,
        case
            when max(selection_coefficient) > 3.00                 then 'High Risk'
            when max(selection_coefficient) between 1.51 and 3.00  then 'Standard'
            when max(selection_coefficient) between 1.01 and 1.50  then 'Safe'
            else 'Unknown'
        end                                                         as ticket_odds_bucket,

        -- highest odd across WON selections only
        max(case when lower(odd_status) = 'win' or lower(odd_status) = 'unspecified' or lower(odd_status) = 'active' 
            then selection_coefficient end)                         as max_won_selection_coefficient,
        case
            when max(case when lower(odd_status) = 'win' or lower(odd_status) = 'unspecified' or lower(odd_status) = 'active' 
                then selection_coefficient end) > 3.00             then 'High Risk'
            when max(case when lower(odd_status) = 'win' or lower(odd_status) = 'unspecified' or lower(odd_status) = 'active' 
                then selection_coefficient end) between 1.51 and 3.00 then 'Standard'
            when max(case when lower(odd_status) = 'win' or lower(odd_status) = 'unspecified' or lower(odd_status) = 'active' 
                then selection_coefficient end) between 1.01 and 1.50 then 'Safe'
            else 'Unknown'
        end                                                         as won_odds_bucket

    from selections
    group by all
)

select
    player_id,
    cc.ticket_code,
    total_selections,
    max_selection_coefficient,
    ticket_odds_bucket,
    max_won_selection_coefficient,
    won_odds_bucket,
    ff.status_reason
from ticket_odds_classified cc
left join (select ticket_code, status_reason from PROD.DWH.F_TICKET
where business_domain_id = 3 group by all) ff
    on cc.ticket_code = ff.ticket_code
