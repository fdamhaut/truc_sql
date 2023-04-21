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


DELETE
FROM sale_order_log
WHERE event_type = '3_transfer' AND (amount_signed = 0 AND recurring_monthly = 0);

DELETE
FROM sale_order_log log
JOIN sale_order next ON next.subscription_id = log.id 
WHERE log.event_date > next.date_order;

UPDATE sale_order_log log
SET event_type = '3_transfer'
    event_date = so.date_order
FROM sale_order so
WHERE so.id = log.order_id
AND log.id IN (
    SELECT DISTINCT ON (order_id) id
    FROM sale_order_log
    WHERE origin_order_id != order_id
    ORDER BY order_id, create_date, event_type
);

UPDATE sale_order_log log
SET event_type = '1_expansion'
FROM sale_order so
WHERE so.id = log.order_id
AND event_type = '3_transfer'
AND log.id NOT IN (
    SELECT DISTINCT ON (order_id) id
    FROM sale_order_log
    WHERE origin_order_id != order_id
    ORDER BY order_id, create_date, event_type DESC
)
AND log.id NOT IN (
    SELECT DISTINCT ON (order_id) id
    FROM sale_order_log
    WHERE origin_order_id != order_id
    ORDER BY order_id, event_date DESC, event_type DESC
);

-- End transfer event
WITH SO AS (
    SELECT so.id, COALESCE(so.origin_order_id, child.origin_order_id) AS origin_order_id , 
    so.client_order_ref, child.date_order, so.currency_id, COALESCE(log.recurring_monthly, 0) AS rm
    FROM sale_order so
    JOIN sale_order child ON child.subscription_id = so.id
    LEFT JOIN (
        SELECT DISTINCT ON (order_id) id, order_id, recurring_monthly
        FROM sale_order_log
        ORDER BY order_id, id DESC
        ) log ON log.order_id = so.id
    WHERE so.subscription_state = '5_renewed'
    AND so.state IN ('sale', 'done')
    AND child.subscription_state IS NOT NULL
    AND child.subscription_state NOT IN ('7_upsell', '1_draft', '2_renewal')
    AND so.id not in (
        SELECT order_id 
        FROM sale_order_log
        WHERE event_type = '2_churn')
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
    '5_renewed',
    '0',
    -SO.rm,
    '0',
    SO.rm,
    '3_transfer'
FROM SO;


----- LIMIT TEST




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

-- Merge multiple expansion/contraction log happening on the same day for the same SO (not contract)
-- First SUM the added users
WITH sum AS (
    SELECT order_id, event_date, COALESCE(referrer_id, -1) as referrer_id, SUM(COALESCE(new_enterprise_user, 0)) as user -----SUM MRR CHANGE ?
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

-- -- Recompute end_date based on create_date
-- WITH last AS (
--     SELECT origin_order_id,
--            MAX(event_date) AS date
--     FROM sale_order_log
--     WHERE event_type != '2_churn'
--     GROUP BY origin_order_id
-- )
-- UPDATE sale_order_log
-- SET event_date = GREATEST(sale_order_log.create_date::date, last.date)
-- FROM last
-- WHERE sale_order_log.event_type = '2_churn' AND sale_order_log.origin_order_id = last.origin_order_id;

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




-- -- Close contract with no active SO and empty MRR
-- WITH last AS (
--     SELECT DISTINCT ON (origin_order_id) id
--     FROM sale_order_log
--     ORDER BY origin_order_id, order_id DESC, event_date DESC, event_type
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
--     '6_churn',
--     '0',
--     '0',
--     '0',
--     '0',
--     '2_churn'
-- FROM sale_order_log log
-- JOIN last ON last.id = log.id
-- WHERE log.recurring_monthly = 0
-- AND event_type IN ('1_expansion', '15_contraction');



UPDATE sale_order
SET state = 'done'
WHERE id IN (
    SELECT id, client_order_ref
    FROM sale_order_bcp so 
    WHERE so.subscription_state = '5_renewed'
    AND id NOT IN (
        SELECT DISTINCT subscription_id
        FROM sale_order_bcp
        WHERE subscription_id IS NOT NULL
    )
);


WITH origin AS (
    SELECT COALESCE(origin_order_id, id) as id, client_order_ref as subscription_code
    FROM sale_order
    WHERE subscription_state IN ('3_progress', '4_paused')
),
log AS (
    SELECT origin_order_id as id, subscription_code
    FROM sale_order_log
    WHERE event_type = '0_creation'
    AND origin_order_id NOT IN (
        SELECT origin_order_id
        FROM sale_order_log
        WHERE event_type = '2_churn'
    )
)
SELECT id, subscription_code
FROM origin
WHERE id NOT IN (
    SELECT id
    FROM log
);



SELECT id, subscription_code
FROM (
    SELECT id, client_order_ref as subscription_code
    FROM sale_order
    WHERE subscription_state IN ('3_progress', '4_paused')
    AND recurring_monthly > 0
) o
WHERE id NOT IN (
    SELECT order_id as id
    FROM sale_order_log
);



SELECT origin_order_id
FROM (
    SELECT origin_order_id, count(*) as c
    FROM sale_order_log
    WHERE event_type = '0_creation'
    GROUP BY origin_order_id
) i
where c > 1;



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
WHERE new.id = sale_order_log.id 
AND event_type != '3_transfer'
AND amount_signed != COALESCE(new.as, recurring_monthly);

WITH new AS (
    SELECT 
        recurring_monthly - LAG(recurring_monthly) 
        OVER (PARTITION BY origin_order_id, order_id ORDER BY create_date, id) AS as,
        id
    FROM sale_order_log
)
SELECT SUM (amount_signed - COALESCE(new.as, recurring_monthly))
FROM sale_order_log
JOIN new ON new.id = sale_order_log.id 
WHERE amount_signed != COALESCE(new.as, recurring_monthly);


SELECT origin_order_id
FROM (
    SELECT origin_order_id, sum(amount_signed)
    FROM sale_order_log
    WHERE event_type = '3_transfer'
    GROUP BY origin_order_id
) i
WHERE sum != 0;


WITH new AS (
    SELECT origin_order_id, count(*) as c
    FROM sale_order_log
    WHERE event_type = '0_creation'
    GROUP BY origin_order_id
), churn AS (
    SELECT origin_order_id, count(*) as c
    FROM sale_order_log
    WHERE event_type = '2_churn'
    GROUP BY origin_order_id
)
SELECT new.origin_order_id
FROM new 
JOIN churn ON new.origin_order_id = churn.origin_order_id
WHERE churn.c > new.c;


SELECT order_id FROM (
    SELECT order_id, sum(amount_signed) as sum
    FROM sale_order_log
    WHERE order_id IN (
        SELECT id
        FROM sale_order
        WHERE start_date = end_date
    )
    GROUP BY order_id
) i
WHERE sum != 0;


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


select origin_order_id, order_id, id, create_date, event_date, event_type, amount_signed, recurring_monthly, new_enterprise_user from sale_order_log where origin_order_id = 2277795 order by order_id DESC, create_date DESC, id DESC;


BEGIN;
TRUNCATE sale_order_log;

INSERT INTO sale_order_log (SELECT * FROM sale_order_log_p_py);
commit;



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





WITH new AS (
    SELECT origin_order_id, count(*) as c
    FROM sale_order_log
    WHERE event_type = '0_creation'
    GROUP BY origin_order_id
), churn AS (
    SELECT origin_order_id, count(*) as c
    FROM sale_order_log
    WHERE event_type = '2_churn'
    GROUP BY origin_order_id
), SO AS (
    SELECT id, COALESCE(origin_order_id, id) as ooid, client_order_ref
    FROM sale_order
)
SELECT DISTINCT SO.ooid, client_order_ref, new.c as new, churn.c as churn
FROM SO
LEFT JOIN new ON new.origin_order_id = SO.ooid 
JOIN churn ON churn.origin_order_id = SO.ooid
WHERE COALESCE(new.c, 0) < churn.c;



WITH cp as (
    SELECT id, subscription_id AS pid
    FROM sale_order 
    WHERE subscription_state IN ('3_progress', '4_paused', '5_renewed', '6_churn')
), ch AS (
    SELECT DISTINCT one.id
    FROM cp one
    JOIN cp two ON one.pid = two.pid
    WHERE one.id != two.id
)
UPDATE sale_order_log log
SET event_type = '0_creation'
FROM ch 
WHERE log.order_id IN (
    SELECT * FROM ch
)
AND event_type = '3_transfer'
AND recurring_monthly = amount_signed;



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