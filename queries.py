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