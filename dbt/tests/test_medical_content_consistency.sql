-- Custom test to ensure medical content consistency
-- This test should return 0 rows to pass

WITH medical_content_check AS (
    SELECT 
        channel_username,
        COUNT(*) as total_messages,
        COUNT(CASE WHEN is_medical_related THEN 1 END) as medical_messages,
        ROUND((COUNT(CASE WHEN is_medical_related THEN 1 END)::FLOAT / COUNT(*)) * 100, 2) as medical_percentage
    FROM {{ ref('stg_telegram_messages') }}
    GROUP BY channel_username
    HAVING COUNT(*) > 10  -- Only check channels with more than 10 messages
)

SELECT 
    channel_username,
    total_messages,
    medical_messages,
    medical_percentage
FROM medical_content_check
WHERE medical_percentage > 90  -- Flag channels with suspiciously high medical content
   OR (total_messages > 50 AND medical_percentage < 1)  -- Flag channels with suspiciously low medical content 