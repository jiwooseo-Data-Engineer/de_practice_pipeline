DAU_QUERY = """
SELECT 
    event_date,
    COUNT(DISTINCT user_id) AS dau
FROM user_log
WHERE event_type IN ('login', 'click')
GROUP BY event_date
ORDER BY event_date;
"""

CTR_QUERY = """
SELECT
    event_date,
    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) /
    NULLIF(SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END), 0) AS ctr
FROM ad_log
GROUP BY event_date
ORDER BY event_date;
"""

CVR_QUERY = """
SELECT
    a.event_date,
    COUNT(DISTINCT o.user_id) /
    NULLIF(COUNT(DISTINCT a.user_id), 0) AS cvr
FROM ad_log a
LEFT JOIN order_log o
    ON a.user_id = o.user_id
    AND o.order_time >= a.event_time
WHERE a.event_type = 'click'
GROUP BY a.event_date
"""

ARPU_QUERY = """
SELECT
    a.event_date,
    SUM(o.amount) /
    NULLIF(COUNT(DISTINCT a.user_id), 0) AS arpu
FROM ad_log a
LEFT JOIN order_log o
    ON a.user_id = o.user_id
    AND o.order_time >= a.event_time
WHERE a.event_type = 'click'
GROUP BY a.event_date
"""