BEGIN;

-- Remove what is after churn
WITH churns AS (
    SELECT id, order_id, event_date, create_date
    FROM sale_order_log
    WHERE event_type = '2_churn'
), after AS (
    SELECT c.id as churn_id, sum(new_enterprise_user) as sum_ent
    FROM sale_order_log log
    JOIN churns c ON c.order_id = log.order_id
    AND log.event_date > c.event_date
    GROUP BY c.id
)
UPDATE sale_order_log log
SET new_enterprise_user = new_enterprise_user + after.sum_ent
FROM after
WHERE after.churn_id = log.id;

WITH churns AS (
    SELECT id, order_id, event_date, create_date
    FROM sale_order_log
    WHERE event_type = '2_churn'
), log AS (
    SELECT log.id
    FROM sale_order_log log
    JOIN churns c ON c.order_id = log.order_id
    WHERE log.event_date > c.event_date
)
DELETE FROM sale_order_log
where id IN (SELECT * FROM log);

-- Reconcile based on SO
WITH SO AS (
    SELECT DISTINCT ON (log.order_id) log.id, so.recurring_monthly, so.subscription_state, so.currency_id
    FROM sale_order_log log
    JOIN sale_order so ON so.id = log.order_id
    WHERE so.subscription_state IN ('3_progress', '4_paused')
    ORDER BY log.order_id, event_date DESC, log.create_date DESC, id DESC
)
INSERT INTO sale_order_log (
	company_id,
    user_id,
    team_id,
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
    log.company_id,
    log.user_id,
    log.team_id,
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

-- Set churn to 0
UPDATE sale_order_log
SET recurring_monthly = 0
WHERE event_type = '2_churn' 
AND recurring_monthly != 0;

-- last log of closed are set to 0 M21011822651180
WITH last AS (
    SELECT DISTINCT ON (order_id) id
    FROM sale_order_log
    WHERE order_id IN (
        SELECT id
        FROM sale_order 
        WHERE subscription_state IN ('5_renewed', '6_churn')
    )
    ORDER BY order_id, event_date DESC, create_date DESC, id DESC
)
UPDATE sale_order_log log
SET recurring_monthly = 0
WHERE log.id IN (SELECT * FROM last)
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

-- Currency change
WITH currency_change AS (
    SELECT 
        currency_id - LAG(currency_id) OVER (PARTITION BY order_id ORDER BY event_date, create_date, id) AS cc,
        LAG(currency_id) OVER (PARTITION BY order_id ORDER BY event_date, create_date, id) as old_c,
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
        currency_id - LAG(currency_id) OVER (PARTITION BY order_id ORDER BY event_date, create_date, id) AS cc,
        id as new_id,
        currency_id as c,
        LAG(currency_id) OVER (PARTITION BY order_id ORDER BY event_date, create_date, id) as old_c,
        LAG(recurring_monthly) OVER (PARTITION BY order_id ORDER BY event_date, create_date, id) as old_rm
    FROM sale_order_log
)
INSERT INTO sale_order_log (
	company_id,
    user_id,
    team_id,
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
    log.company_id,
    log.user_id,
    log.team_id,
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

-- Merge multiple expansion/contraction log happening on the same day for the same SO (not contract)
-- First SUM the added users
WITH sum AS (
    SELECT count(1) as c, order_id, date_trunc('month', event_date) as event_date, 
        COALESCE(referrer_id, -1) as referrer_id, 
        SUM(COALESCE(new_enterprise_user, 0)) as user,
        SUM(amount_signed) as a_s,
        currency_id
    FROM sale_order_log
    WHERE event_type IN ('1_expansion', '15_contraction')
    GROUP BY order_id, date_trunc('month', event_date), referrer_id, currency_id
)
UPDATE sale_order_log log
SET new_enterprise_user = sum.user,
    amount_signed = sum.a_s
FROM sum
WHERE log.order_id = sum.order_id 
AND log.currency_id = sum.currency_id
AND c > 1
AND date_trunc('month', log.event_date) = date_trunc('month', sum.event_date)
AND COALESCE(log.referrer_id, -1) = sum.referrer_id
AND event_type IN ('1_expansion', '15_contraction');

-- Then the removal of log
DELETE 
FROM sale_order_log
WHERE event_type IN ('1_expansion', '15_contraction')
AND id NOT IN (
    SELECT DISTINCT ON (order_id, date_trunc('month', event_date), COALESCE(referrer_id, -1), currency_id) id
    FROM sale_order_log
    WHERE event_type IN ('1_expansion', '15_contraction')
    ORDER BY order_id, date_trunc('month', event_date) DESC, COALESCE(referrer_id, -1), currency_id, event_date DESC, create_date DESC, id DESC
);

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

-- Delete empty log
DELETE
FROM sale_order_log
WHERE (amount_signed = 0 OR amount_signed IS NULL)
AND event_type IN ('1_expansion', '15_contraction')
AND (new_enterprise_user = 0 OR new_enterprise_user IS NULL);

-- Ensure every active SO has at least a log
WITH SO AS (
    SELECT *
    FROM sale_order
    WHERE recurring_monthly > 0 
    AND subscription_state IN ('3_progress', '4_paused')
    AND id NOT IN (
        SELECT order_id
        FROM sale_order_log
    )
)
INSERT INTO sale_order_log (
    company_id,
    user_id,
    team_id,
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
    company_id,
    user_id,
    team_id,
    id, 
    origin_order_id,
    client_order_ref, 
    date_order::date,
    date_order,
    currency_id,
    subscription_state,
    recurring_monthly,
    recurring_monthly,
    recurring_monthly,
    0,
    '0_creation'
FROM SO;

COMMIT;
