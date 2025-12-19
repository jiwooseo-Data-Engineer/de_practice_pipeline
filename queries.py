DAU_QUERY = """
SELECT
  %(target_date)s AS event_date,
  COUNT(DISTINCT user_id) AS dau
FROM user_log
WHERE event_type IN ('login', 'click')
  AND event_date = %(target_date)s
"""

CTR_QUERY = """
SELECT
  %(target_date)s AS event_date,
  COALESCE(
    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) * 1.0 /
    NULLIF(SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END), 0),
    0
  ) AS ctr
FROM ad_log
WHERE event_date = %(target_date)s
"""

CVR_QUERY = """
SELECT
  %(target_date)s AS event_date,
  COALESCE(
    (
      SELECT COUNT(DISTINCT c.user_id)
      FROM ad_log c
      JOIN order_log o
        ON o.user_id = c.user_id
       AND DATE(o.order_time) = %(target_date)s
      WHERE c.event_type = 'click'
        AND c.event_date = %(target_date)s
    ) * 1.0
    /
    NULLIF(
      (
        SELECT COUNT(DISTINCT user_id)
        FROM ad_log
        WHERE event_type = 'click'
          AND event_date = %(target_date)s
      ),
      0
    ),
    0
  ) AS cvr
"""

ARPU_QUERY = """
SELECT
  %(target_date)s AS event_date,
  COALESCE(
    (
      SELECT SUM(amount)
      FROM order_log
      WHERE DATE(order_time) = %(target_date)s
    ) * 1.0
    /
    NULLIF(
      (
        SELECT COUNT(DISTINCT user_id)
        FROM user_log
        WHERE event_type IN ('login', 'click')
          AND event_date = %(target_date)s
      ),
      0
    ),
    0
  ) AS arpu
"""
