BEGIN;
TRUNCATE sale_order_log;

INSERT INTO sale_order_log (SELECT * FROM sale_order_log_bcp);

-- TRUNCATE sale_order;

-- INSERT INTO sale_order (SELECT * FROM sale_order_bcp);


UPDATE sale_order
SET subscription_state = NULL
WHERE subscription_state is not NULL AND state = 'cancel';

-- Change renewed with no child to churned M22112156233162, M22112156233162, M22112156233162, M22112156233162
UPDATE sale_order 
SET subscription_state = '6_churn'
WHERE id IN (
    SELECT id
    FROM sale_order so 
    WHERE so.subscription_state = '5_renewed'
    AND id NOT IN (
        SELECT DISTINCT subscription_id
        FROM sale_order
        WHERE subscription_id IS NOT NULL
    )
);

-- Remove log from cancelled SO M1811038904534 M23011767584878 M1809248620518 M1808078312543
DELETE FROM sale_order_log
WHERE order_id IN (
    SELECT id
    FROM sale_order so
    WHERE state IN ('cancel', 'draft', 'sent')
);

-- Correct Error from 8a8080ed4f75c811cf0e92065a86f4723a4aaced M21092030723035 M21092030731668 M21082429835776
UPDATE sale_order_log
SET recurring_monthly = amount_signed,
    amount_signed = recurring_monthly
WHERE amount_signed = 0 
AND event_type IN ('2_churn' , '3_transfer')
AND create_date > '2022-09-06 14:20:41.120188' 
AND create_date < '2023-02-08 10:42:52.359322';
--AND recurring_monthly < amount_signed 


-- Creation that are not the first log are changed into expansion M22031437539225 M22092345498172 M20053016649499 M20111620889854
UPDATE sale_order_log
SET event_type = '1_expansion'
WHERE event_type = '0_creation'
AND id NOT IN (
    SELECT DISTINCT ON (origin_order_id) id
    FROM sale_order_log
    ORDER BY origin_order_id, create_date, event_type
);

-- Churn that are not the last log are changed into contraction M22072142742208 M19103012676864 M20120421642477 M22070942264774
UPDATE sale_order_log
SET event_type = '15_contraction'
WHERE event_type = '2_churn' AND id NOT IN (
    SELECT DISTINCT ON (origin_order_id) id
    FROM sale_order_log
    WHERE event_type != '3_transfer'
    ORDER BY origin_order_id, event_date DESC, event_type DESC
);

-- Churn un progress are removed M22041138671603 M22111755165774 M19072711872346
UPDATE sale_order_log
SET event_type = '15_contraction'
WHERE event_type = '2_churn'
AND order_id IN (
    SELECT id
    FROM sale_order
    WHERE subscription_state IN ('3_progress', '4_paused')
);

-- changer en order by ID ?

-- First log are changed into creation M20042716041309 M1811068925874 M1712306167873 M1705103964518
UPDATE sale_order_log
SET event_type = '0_creation',
    amount_signed = sale_order_log.recurring_monthly,
    event_date = so.date_order
FROM sale_order so
WHERE so.id = sale_order_log.order_id
AND sale_order_log.id IN (
    SELECT DISTINCT ON (origin_order_id) id
    FROM sale_order_log
    ORDER BY origin_order_id, create_date, event_type, id
)
AND sale_order_log.order_id = sale_order_log.origin_order_id
AND event_type != '0_creation';

-- Bring back future log for SO churned/renewed M21113033509360 M160425787283 M21062827904700 M21061527464805
UPDATE sale_order_log
SET event_date = create_date::date
WHERE id IN (
    SELECT log.id
    FROM sale_order_log log
    JOIN sale_order so ON so.id = log.order_id
    WHERE event_date >= '2023-04-13'
    AND so.subscription_state IN ('6_churn', '5_renewed', '3_progress', '4_paused')
);


-- We add churned log to churned SO with no churn log M20092219749813 M1608031437668 M140703666487
WITH SO AS (
    SELECT so.id, COALESCE(end_date, next_invoice_date) as end_date, origin_order_id, client_order_ref, 
        currency_id, subscription_state, l.recurring_monthly as rm
    from sale_order so
    JOIN (
        SELECT DISTINCT ON (order_id) order_id, recurring_monthly, id
        FROM sale_order_log
        ORDER BY order_id, id
        ) l on l.order_id = so.id
    where so.subscription_state = '6_churn'
    and so.state in ('sale', 'done')
    and so.id not in (
        SELECT order_id
        from sale_order_log
        where event_type = '2_churn'
        AND order_id IS NOT NULL
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
    LEAST(SO.end_date::date, '2022-04-12'),
    SO.currency_id,
    SO.subscription_state,
    '0',
    -SO.rm,
    '0',
    SO.rm,
    '2_churn'
FROM SO;

-- Compute amount_signed if doesn't exist based on recurring_monthly
WITH new AS (
    SELECT 
        recurring_monthly - LAG(recurring_monthly) 
        OVER (PARTITION BY origin_order_id, order_id ORDER BY create_date, id) AS as,
        id
    FROM sale_order_log
)
UPDATE sale_order_log
SET amount_signed = COALESCE(new.as, recurring_monthly)
FROM new 
WHERE new.id = sale_order_log.id AND amount_signed IS NULL;

-- Recompute contraction and expansion value
UPDATE log_bis
SET amount_contraction = -amount_signed,
    amount_expansion = 0,
    event_type = '15_contraction'
WHERE amount_signed < 0 AND event_type = '1_expansion';

UPDATE log_bis
SET amount_contraction = 0,
    amount_expansion = amount_signed,
    event_type = '1_expansion'
WHERE amount_signed > 0 AND event_type = '15_contraction';

