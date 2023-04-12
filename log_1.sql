-- Correct Error from 8a8080ed4f75c811cf0e92065a86f4723a4aaced
UPDATE sale_order_log
SET recurring_monthly = amount_signed,
    amount_signed = recurring_monthly
WHERE recurring_monthly < amount_signed 
AND amount_signed = 0 
AND event_type IN ('2_churn' , '3_transfer')
AND create_date > '2022-09-06 14:20:41.120188' 
AND create_date < '2023-02-08 10:42:52.359322';


-- Push event_date to ensure cohesion
UPDATE sale_order_log log
SET event_date = so.start_date
FROM sale_order so
WHERE so.id = log.order_id
  AND event_date < so.start_date;

-- Creation that are not the first log are changed into expansion
UPDATE sale_order_log
SET event_type = '1_expansion'
WHERE event_type = '0_creation'
AND id NOT IN (
    SELECT DISTINCT ON (origin_order_id) id
    FROM sale_order_log
    ORDER BY origin_order_id, create_date, event_type
);

-- Churn that are not the last log are changed into contraction
UPDATE sale_order_log
SET event_type = '15_contraction'
WHERE event_type = '2_churn' AND id NOT IN (
    SELECT DISTINCT ON (origin_order_id) id
    FROM sale_order_log
    ORDER BY origin_order_id, event_date DESC, event_type DESC
);

-- First log are changed into creation
UPDATE sale_order_log
SET event_type = '0_creation',
    amount_signed = sale_order_log.recurring_monthly,
    event_date = so.date_order
FROM sale_order so
WHERE so.id = sale_order_log.order_id
AND so.state = 'cancel'
AND sale_order_log.id IN (
    SELECT DISTINCT ON (origin_order_id) id
    FROM sale_order_log
    ORDER BY origin_order_id, create_date, event_type, id
)
AND sale_order_log.order_id = sale_order_log.origin_order_id
AND event_type != '0_creation';

-- Delete future log for SO in progress/paused
DELETE FROM sale_order_log
WHERE id IN (
    SELECT log.id
    FROM sale_order_log log
    JOIN sale_order so ON so.id = log.order_id
    WHERE event_date > '2023-04-12'
    AND so.subscription_state IN ('3_progress', '4_paused')
);

-- Bring back future log for SO churned/renewed
UPDATE sale_order_log
SET event_date = create_date::date
WHERE id IN (
    SELECT log.id
    FROM sale_order_log log
    JOIN sale_order so ON so.id = log.order_id
    WHERE event_date > '2023-04-12'
    AND so.subscription_state IN ('6_churned', '5_renewed')
);

-- Remove log from cancelled SO
DELETE FROM sale_order_log
WHERE order_id IN (
    SELECT id
    FROM sale_order so
    WHERE state IN ('cancel', 'draft')
);


-- We correct ensure the first created log of each SO (except the first SO) is transfer 
WITH SO as (
    SELECT id, date_order, origin_order_id, client_order_ref, 
        currency_id, subscription_state, recurring_monthly
    FROM sale_order
    WHERE subscription_state IN ('3_progress', '4_paused')
    AND id NOT IN (
        SELECT order_id
        FROM sale_order_log
        WHERE (event_type = '3_transfer' AND recurring_monthly = amount_signed)
         OR event_type = '0_creation'
    )
)
INSERT INTO sale_order_log (
    order_id,
    origin_order_id,
    subscription_code,
    event_date,
    currency_id,
    subscription_state,
    recurring_monthly,
    amount_signed,
    amount_expansion,
    amount_contraction,
    event_type
)
SELECT 
    SO.id, 
    SO.origin_order_id,
    SO.client_order_ref, 
    SO.date_order::date,
    SO.currency_id,
    SO.subscription_state,
    SO.recurring_monthly,
    SO.recurring_monthly,
    SO.recurring_monthly,
    '0',
    '3_transfer'
FROM SO;

-- And that the last log of each SO (except the last SO) is also a transfer
UPDATE sale_order_log
SET event_type = '3_transfer'
WHERE id IN (
    SELECT DISTINCT ON (order_id) id
    FROM sale_order_log
    WHERE recurring_monthly = 0
    ORDER BY order_id, event_date DESC, event_type DESC
)
AND id NOT IN (
    SELECT DISTINCT ON (origin_order_id) id
    FROM sale_order_log
    WHERE recurring_monthly = 0
    ORDER BY origin_order_id, event_date DESC, event_type DESC
)
AND order_id NOT IN (
    SELECT order_id
    FROM sale_order_log
    where event_type = '3_transfer'
    and recurring_monthly = 0
);

-- Push event_date to ensure cohesion
UPDATE sale_order_log log
SET event_date = create_date::date;

-- Merge multiple expansion/contraction log happening on the same day for the same SO (not contract)
-- First SUM the added users
WITH sum AS (
    SELECT order_id, event_date, COALESCE(referrer_id, -1) as referrer_id, SUM(COALESCE(new_enterprise_user, 0)) as user
    FROM sale_order_log
    WHERE event_type IN ('1_expansion', '15_contraction')
    GROUP BY order_id, event_date, referrer_id
)
UPDATE sale_order_log log
SET new_enterprise_user = sum.user
FROM sum
WHERE log.order_id = sum.order_id 
AND log.event_date = sum.event_date
AND COALESCE(log.referrer_id, -1) = sum.referrer_id
AND event_type IN ('1_expansion', '15_contraction');

-- Then the removal of log
DELETE 
FROM sale_order_log
WHERE event_type IN ('1_expansion', '15_contraction')
AND id NOT IN (
    SELECT DISTINCT ON (order_id, event_date, referrer_id) id
    FROM sale_order_log
    WHERE event_type IN ('1_expansion', '15_contraction')
    ORDER BY order_id, event_date DESC, referrer_id, create_date DESC
);
--END MERGE

-- Compute amount_signed based on recurring_monthly
WITH new AS (
    SELECT 
        recurring_monthly - LAG(recurring_monthly) 
        OVER (PARTITION BY origin_order_id, order_id ORDER BY create_date, event_date, event_type) AS as,
        id
    FROM sale_order_log
)
UPDATE sale_order_log
SET amount_signed = COALESCE(new.as, recurring_monthly)
FROM new 
WHERE new.id = sale_order_log.id;

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

-- Recompute end_date based on create_date
WITH last AS (
    SELECT origin_order_id,
           MAX(event_date) AS date
    FROM sale_order_log
    WHERE event_type != '2_churn'
    GROUP BY origin_order_id
)
UPDATE sale_order_log
SET event_date = GREATEST(sale_order_log.create_date::date, last.date)
FROM last
WHERE sale_order_log.event_type = '2_churn' AND sale_order_log.origin_order_id = last.origin_order_id;

-- -- Reconcile based on SO
-- WITH last AS (
--     SELECT DISTINCT ON (log.origin_order_id) log.id, so.recurring_monthly, so.subscription_state
--     FROM sale_order_log log
--     JOIN sale_order so ON so.id = log.order_id
--     WHERE so.subscription_state IN ('3_progress', '4_paused')
--     ORDER BY log.origin_order_id, event_date DESC, event_type DESC
-- )
-- INSERT INTO sale_order_log (
--     order_id,
--     origin_order_id,
--     subscription_code,
--     event_date,
--     currency_id,
--     subscription_state,
--     recurring_monthly,
--     amount_signed,
--     amount_expansion,
--     amount_contraction,
--     event_type
-- )
-- SELECT 
--     log.order_id, 
--     log.origin_order_id, 
--     log.subscription_code,
--     log.event_date,
--     log.currency_id,
--     last.subscription_state,
--     last.recurring_monthly,
--     last.recurring_monthly - log.recurring_monthly ,
--     GREATEST(last.recurring_monthly - log.recurring_monthly, 0),
--     GREATEST(-last.recurring_monthly + log.recurring_monthly, 0),
--     CASE WHEN last.recurring_monthly - log.recurring_monthly > 0 THEN '1_expansion' ELSE '15_contraction' END
-- FROM sale_order_log log
-- JOIN last ON last.id = log.id
-- WHERE last.recurring_monthly != log.recurring_monthly;

-- Close contract with no active SO and empty MRR
WITH last AS (
    SELECT DISTINCT ON (origin_order_id) id
    FROM sale_order_log
    ORDER BY origin_order_id, order_id DESC, event_date DESC, event_type
)
INSERT INTO sale_order_log (
    order_id,
    origin_order_id,
    subscription_code,
    event_date,
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
    log.currency_id,
    '6_churn',
    '0',
    '0',
    '0',
    '0',
    '2_churn'
FROM sale_order_log log
JOIN last ON last.id = log.id
WHERE log.recurring_monthly = 0
AND event_type IN ('1_expansion', '15_contraction');
