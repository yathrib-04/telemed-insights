-- Custom test to validate engagement score consistency
-- This test should return 0 rows to pass

WITH engagement_anomalies AS (
    SELECT 
        message_id,
        channel_username,
        views,
        forwards,
        replies,
        engagement_score,
        -- Calculate expected engagement score
        COALESCE(views, 0) + (COALESCE(forwards, 0) * 2) + (COALESCE(replies, 0) * 3) as expected_engagement_score,
        -- Check for suspicious patterns
        CASE 
            WHEN views > 10000 AND forwards = 0 AND replies = 0 THEN 'High views, no engagement'
            WHEN forwards > 1000 THEN 'Suspiciously high forwards'
            WHEN replies > 500 THEN 'Suspiciously high replies'
            WHEN engagement_score < 0 THEN 'Negative engagement score'
            ELSE 'OK'
        END as anomaly_type
    FROM {{ ref('stg_telegram_messages') }}
    WHERE message_id IS NOT NULL
)

SELECT 
    message_id,
    channel_username,
    views,
    forwards,
    replies,
    engagement_score,
    expected_engagement_score,
    ABS(engagement_score - expected_engagement_score) as score_difference,
    anomaly_type
FROM engagement_anomalies
WHERE anomaly_type != 'OK'
   OR ABS(engagement_score - expected_engagement_score) > 1  -- Allow for small rounding differences 