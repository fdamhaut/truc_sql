BEGIN;

-- MERGE void expansion and contraction with no change in MRR (Newly created in python)

WITH change AS (
    SELECT DISTINCT ON (ch.id) st.id as id, ch.id as change_id, 
    	COALESCE(st.new_enterprise_user, 0) + COALESCE(ch.new_enterprise_user, 0) as ent_user
    FROM sale_order_log st
    JOIN sale_order_log ch ON (st.create_date = ch.create_date AND st.order_id = ch.order_id)
    WHERE st.event_type IN ('3_transfer', '0_creation', '2_churn')
    AND ch.event_type IN ('1_expansion', '15_contraction')
    AND st.recurring_monthly = ch.recurring_monthly
    ORDER BY ch.id
)
UPDATE sale_order_log
SET new_enterprise_user = ch.ent_user
FROM change ch
WHERE sale_order_log.id = ch.id;

DELETE 
FROM sale_order_log
WHERE id IN (
    SELECT ch.id
    FROM sale_order_log st
    JOIN sale_order_log ch ON (st.create_date = ch.create_date AND st.order_id = ch.order_id)
    WHERE st.event_type IN ('3_transfer', '0_creation', '2_churn')
    AND ch.event_type IN ('1_expansion', '15_contraction')
    AND st.recurring_monthly = ch.recurring_monthly
    ORDER BY ch.id
);


-- Reconcile based on SO
WITH last AS (
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
    last.currency_id,
    last.subscription_state,
    last.recurring_monthly,
    last.recurring_monthly - log.recurring_monthly ,
    GREATEST(last.recurring_monthly - log.recurring_monthly, 0),
    GREATEST(-last.recurring_monthly + log.recurring_monthly, 0),
    CASE WHEN last.recurring_monthly - log.recurring_monthly > 0 THEN '1_expansion' ELSE '15_contraction' END
FROM sale_order_log log
JOIN last ON last.id = log.id
WHERE last.recurring_monthly != log.recurring_monthly;


-- Reconcile based on SO
WITH last AS (
    SELECT DISTINCT ON (log.order_id) log.id, 0 as recurring_monthly, so.subscription_state, so.currency_id
    FROM sale_order_log log
    JOIN sale_order so ON so.id = log.order_id
    WHERE so.subscription_state IN ('6_churn')
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
    last.currency_id,
    last.subscription_state,
    last.recurring_monthly,
    last.recurring_monthly - log.recurring_monthly ,
    GREATEST(last.recurring_monthly - log.recurring_monthly, 0),
    GREATEST(-last.recurring_monthly + log.recurring_monthly, 0),
    CASE WHEN last.recurring_monthly - log.recurring_monthly > 0 THEN '1_expansion' ELSE '15_contraction' END
FROM sale_order_log log
JOIN last ON last.id = log.id
WHERE last.recurring_monthly != log.recurring_monthly;


-- Compute amount_signed based on recurring_monthly
WITH new AS (
    SELECT 
        recurring_monthly - LAG(recurring_monthly) 
        OVER (PARTITION BY order_id ORDER BY create_date, id) AS as,
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


COMMIT;


-- -- Merge multiple expansion/contraction log happening on the same day for the same SO (not contract)
-- -- First SUM the added users
-- WITH sum AS (
--     SELECT order_id, event_date, COALESCE(referrer_id, -1) as referrer_id, SUM(COALESCE(new_enterprise_user, 0)) as user
--     FROM sale_order_log
--     WHERE event_type IN ('1_expansion', '15_contraction')
--     GROUP BY order_id, event_date, referrer_id
-- )
-- UPDATE sale_order_log log
-- SET new_enterprise_user = sum.user
-- FROM sum
-- WHERE log.order_id = sum.order_id 
-- AND log.event_date = sum.event_date
-- AND COALESCE(log.referrer_id, -1) = sum.referrer_id
-- AND event_type IN ('1_expansion', '15_contraction');

-- -- Then the removal of log
-- DELETE 
-- FROM sale_order_log
-- WHERE event_type IN ('1_expansion', '15_contraction')
-- AND id NOT IN (
--     SELECT DISTINCT ON (order_id, event_date, referrer_id) id
--     FROM sale_order_log
--     WHERE event_type IN ('1_expansion', '15_contraction')
--     ORDER BY order_id, event_date DESC, referrer_id, create_date DESC
-- );
-- --END MERGE

-- -- Compute amount_signed based on recurring_monthly
-- WITH new AS (
--     SELECT 
--         recurring_monthly - LAG(recurring_monthly) 
--         OVER (PARTITION BY origin_order_id, order_id ORDER BY create_date, event_date, event_type) AS as,
--         id
--     FROM sale_order_log
-- )
-- UPDATE sale_order_log
-- SET amount_signed = COALESCE(new.as, recurring_monthly)
-- FROM new 
-- WHERE new.id = sale_order_log.id;

-- -- Delete empty log
-- DELETE
-- FROM sale_order_log
-- WHERE (amount_signed = 0 OR amount_signed IS NULL)
-- AND event_type IN ('1_expansion', '15_contraction')
-- AND (new_enterprise_user = 0 OR new_enterprise_user IS NULL);

-- -- Recompute contraction and expansion value
-- UPDATE sale_order_log
-- SET amount_contraction = -amount_signed,
--     amount_expansion = 0,
--     event_type = '15_contraction'
-- WHERE amount_signed < 0 AND event_type = '1_expansion';

-- UPDATE sale_order_log
-- SET amount_contraction = 0,
--     amount_expansion = amount_signed,
--     event_type = '1_expansion'
-- WHERE amount_signed > 0 AND event_type = '15_contraction';
