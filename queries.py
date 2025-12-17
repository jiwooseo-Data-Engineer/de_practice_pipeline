DAU_QUERY = """
SELECT 
    event_date,
    COUNT(DISTINCT user_id) AS dau
FROM user_log
WHERE event_type IN ('login', 'click')
  AND event_date = :target_date
GROUP BY event_date
"""
CTR_QUERY = """
SELECT
    event_date,
    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) * 1.0 /
    NULLIF(SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END), 0) AS ctr
FROM ad_log
WHERE event_date = :target_date
GROUP BY event_date
"""

CVR_QUERY = """
SELECT
    a.event_date,
    COUNT(DISTINCT o.user_id) * 1.0 /
    NULLIF(COUNT(DISTINCT a.user_id), 0) AS cvr
FROM ad_log a
LEFT JOIN order_log o
    ON a.user_id = o.user_id
    AND o.order_time >= a.event_time
WHERE a.event_type = 'click'
  AND a.event_date = :target_date
GROUP BY a.event_date
"""

ARPU_QUERY = """
SELECT
    u.event_date,
    SUM(o.amount) * 1.0 /
    NULLIF(COUNT(DISTINCT u.user_id), 0) AS arpu
FROM user_log u
LEFT JOIN order_log o
    ON u.user_id = o.user_id
    AND DATE(o.order_time) = u.event_date
WHERE u.event_type IN ('login', 'click')
  AND u.event_date = :target_date
GROUP BY u.event_date
"""