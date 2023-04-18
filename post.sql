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
    SELECT DISTINCT ON (log.order_id) log.id, so.recurring_monthly, so.subscription_state
    FROM sale_order_log log
    JOIN sale_order so ON so.id = log.order_id
    WHERE so.subscription_state IN ('3_progress', '4_paused')
    ORDER BY log.order_id, event_date DESC, event_type DESC, create_date DESC, id DESC
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
    log.currency_id,
    last.subscription_state,
    last.recurring_monthly,
    last.recurring_monthly - log.recurring_monthly ,
    GREATEST(last.recurring_monthly - log.recurring_monthly, 0),
    GREATEST(-last.recurring_monthly + log.recurring_monthly, 0),
    CASE WHEN last.recurring_monthly - log.recurring_monthly > 0 THEN '1_expansion' ELSE '15_contraction' END
FROM sale_order_log log
JOIN last ON last.id = log.id
WHERE last.recurring_monthly != log.recurring_monthly;


-- Compute amount_signed if doesn't exist based on recurring_monthly
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
WHERE new.id = sale_order_log.id;