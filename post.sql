BEGIN;

-- Reconcile based on SO
WITH SO AS (
    SELECT DISTINCT ON (log.order_id) log.id, so.recurring_monthly, so.subscription_state, so.currency_id
    FROM sale_order_log log
    JOIN sale_order so ON so.id = log.order_id
    WHERE so.subscription_state IN ('3_progress', '4_paused')
    ORDER BY log.order_id, event_date DESC, event_type DESC, log.create_date DESC, id DESC
)
INSERT INTO sale_order_log (
    order_id,
    origin_order_id,
    subscription_code,
    event_date,
    create_date,
    currency_id,
    subscription_state,
    recurring_monthly,
    amount_signed,
    amount_expansion,
    amount_contraction,
    event_type
)
SELECT 
    log.order_id, 
    log.origin_order_id, 
    log.subscription_code,
    log.event_date,
    log.create_date,
    SO.currency_id,
    SO.subscription_state,
    SO.recurring_monthly,
    SO.recurring_monthly - log.recurring_monthly ,
    GREATEST(SO.recurring_monthly - log.recurring_monthly, 0),
    GREATEST(-SO.recurring_monthly + log.recurring_monthly, 0),
    CASE WHEN SO.recurring_monthly - log.recurring_monthly > 0 THEN '1_expansion' ELSE '15_contraction' END
FROM sale_order_log log
JOIN SO ON SO.id = log.id
WHERE SO.recurring_monthly != log.recurring_monthly OR SO.currency_id != log.currency_id;


UPDATE sale_order_log
SET recurring_monthly = 0
WHERE event_type = '2_churn' 
AND recurring_monthly != 0;

-- Compute amount_signed based on recurring_monthly
WITH new AS (
    SELECT 
        recurring_monthly - LAG(recurring_monthly) 
        OVER (PARTITION BY order_id ORDER BY event_date, create_date, id) AS as,
        id
    FROM sale_order_log
)
UPDATE sale_order_log
SET amount_signed = COALESCE(new.as, recurring_monthly)
FROM new
WHERE new.id = sale_order_log.id 
AND amount_signed != COALESCE(new.as, recurring_monthly);

-- Delete empty log
DELETE
FROM sale_order_log
WHERE (amount_signed = 0 OR amount_signed IS NULL)
AND event_type IN ('1_expansion', '15_contraction')
AND (new_enterprise_user = 0 OR new_enterprise_user IS NULL);

-- Recompute contraction and expansion value
UPDATE sale_order_log
SET amount_contraction = -amount_signed,
    amount_expansion = 0,
    event_type = '15_contraction'
WHERE amount_signed < 0 AND event_type = '1_expansion';

UPDATE sale_order_log
SET amount_contraction = 0,
    amount_expansion = amount_signed,
    event_type = '1_expansion'
WHERE amount_signed > 0 AND event_type = '15_contraction';

-- Currency change
WITH currency_change AS (
    SELECT 
        currency_id - LAG(currency_id) OVER (PARTITION BY order_id ORDER BY create_date, id) AS cc,
        LAG(currency_id) OVER (PARTITION BY order_id ORDER BY create_date, id) as old_c,
        id as new_id
    FROM sale_order_log
)
UPDATE sale_order_log log
SET amount_signed = recurring_monthly
FROM currency_change cc
WHERE cc.new_id = log.id
AND cc.cc != 0
AND cc.old_c IS NOT NULL
AND recurring_monthly != amount_signed;

WITH currency_change AS (
    SELECT 
        currency_id - LAG(currency_id) OVER (PARTITION BY order_id ORDER BY create_date, id) AS cc,
        id as new_id,
        currency_id as c,
        LAG(currency_id) OVER (PARTITION BY order_id ORDER BY create_date, id) as old_c,
        LAG(recurring_monthly) OVER (PARTITION BY order_id ORDER BY create_date, id) as old_rm
    FROM sale_order_log
)
INSERT INTO sale_order_log (
    order_id,
    origin_order_id,
    subscription_code,
    event_date,
    create_date,
    currency_id,
    subscription_state,
    recurring_monthly,
    amount_signed,
    amount_expansion,
    amount_contraction,
    event_type
)
SELECT 
    log.order_id, 
    log.origin_order_id, 
    log.subscription_code,
    log.event_date,
    log.create_date,
    cc.old_c,
    log.subscription_state,
    0,
    -cc.old_rm,
    0,
    cc.old_rm,
    '15_contraction'
FROM sale_order_log log
JOIN currency_change cc ON log.id = cc.new_id
WHERE cc.cc != 0 
AND cc.old_c IS NOT NULL
AND cc.old_rm != 0; 

COMMIT;
