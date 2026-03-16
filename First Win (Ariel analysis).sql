with ticket_deduped as (
    select
        player_id,
        convert_timezone('UTC', 'CET', accepted_dt)  as ticket_dt,
        lower(ticket_status)                          as ticket_status,
        ticket_code,
        max(payout_total_amt)                         as payout_total_amt
    from PROD.DWH.F_TICKET
    where business_domain_id = 3
        and lower(ticket_status) <> 'cancelled'
    group by all
),

ticket_ranked as (
    select
        player_id,
        ticket_dt,
        ticket_status,
        ticket_code,
        payout_total_amt,
        row_number() over (
            partition by player_id
            order by ticket_dt asc
        ) as ticket_rank_overall
    from ticket_deduped
),

first_win as (
    select
        player_id,
        ticket_dt                                    as first_win_dt,
        ticket_code                                  as first_win_ticket_code,
        payout_total_amt                             as first_win_payout_amt,
        ticket_rank_overall                          as first_win_rank,
        ticket_rank_overall - 1                      as lost_tickets_before_first_win
    from ticket_ranked
    where lower(ticket_status) = 'win'
    qualify row_number() over (
        partition by player_id 
        order by ticket_dt asc
    ) = 1
)

select
    player_id,
    first_win_dt,
    first_win_ticket_code,
    first_win_payout_amt,
    first_win_rank,
    lost_tickets_before_first_win,
    case 
        when lost_tickets_before_first_win = 0  then 'Won on 1st ticket'
        when lost_tickets_before_first_win between 1 and 2  then '1-2 losses before win'
        when lost_tickets_before_first_win between 3 and 5  then '3-5 losses before win'
        when lost_tickets_before_first_win between 6 and 10 then '6-10 losses before win'
        else '10+ losses before win'
    end                                              as first_win_sequence_bucket,
    first_win_dt >= dateadd('month', -6, date_trunc('month', current_date())) as is_recent_cohort
from first_win
where first_win_dt >= dateadd('month', -6, date_trunc('month', current_date()))

