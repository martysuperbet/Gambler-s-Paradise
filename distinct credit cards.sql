--distinct credit cards from f_payment

SELECT 
  player_id,
  COUNT(*) AS distinct_cards,
FROM (
SELECT 
    player_id,
    REGEXP_SUBSTR(card_number, '[0-9]{4}$') AS last_four_digits,
    MIN(card_number) AS card_exact_number  -- Takes first alphabetically/simplest format
  FROM PROD.DWH.F_PAYMENT_TRANSACTION
  WHERE business_domain_id = 3 
    AND card_number NOT IN ('UNK', '')
    AND payment_status = 'approved'
    AND payment_amount >= 0.01
  GROUP BY all
)
GROUP BY 1
