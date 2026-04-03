with 
payment_type as (
select 
    player_id,
    PAYMENT_PROVIDER_TRANSACTION_ID, --src_transaction_id
    payment_type,
    payment_transaction_id,
    payment_transaction_code,
    merchant_transaction_id,
    --PAYMENT_PROVIDER_TRANSACTION_ID
from PROD.DWH.F_PAYMENT_TRANSACTION
where business_domain_id = 3 
group by all
),

kasia as (
select 
    kk.*,

    player_id,
    payment_transaction_id,
    payment_transaction_code,
    PAYMENT_PROVIDER_TRANSACTION_ID,
    merchant_transaction_id,
    bank_transaction_id,
    encrypted_id,
    upo,
    lifecycle_id,
from PROD.BI.PL_JPO_MISSING_KASIA kk
left join payment_type pp
    on to_char(kk.transaction_id) = to_char(pp.PAYMENT_PROVIDER_TRANSACTION_ID)
left join (select
    transaction_id,
    bank_transaction_id,
    user_token_id,
    encrypted_id,
    upo,
    lifecycle_id,
from PROD.ODS_PAYMENT.NUVEI_MASTER_TRANSACTION
group by all) cc
    on to_char(kk.transaction_id) = to_char(cc.transaction_id)
--where player_id = '2042695b-8a04-560f-89b6-84cabffdbd92'
)

select * from kasia
