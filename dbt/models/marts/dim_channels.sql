{{
  config(
    materialized='table'
  )
}}

WITH channel_stats AS (
    SELECT
        channel_username,
        channel_title,
        COUNT(*) as total_messages,
        COUNT(CASE WHEN is_medical_related THEN 1 END) as medical_messages,
        COUNT(CASE WHEN has_image THEN 1 END) as messages_with_images,
        COUNT(CASE WHEN has_document THEN 1 END) as messages_with_documents,
        AVG(message_length) as avg_message_length,
        AVG(engagement_score) as avg_engagement_score,
        SUM(views) as total_views,
        SUM(forwards) as total_forwards,
        SUM(replies) as total_replies,
        MIN(message_date) as first_message_date,
        MAX(message_date) as last_message_date
    FROM {{ ref('stg_telegram_messages') }}
    GROUP BY channel_username, channel_title
),

channel_ranking AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY total_messages DESC) as channel_rank_by_messages,
        ROW_NUMBER() OVER (ORDER BY medical_messages DESC) as channel_rank_by_medical_content,
        ROW_NUMBER() OVER (ORDER BY avg_engagement_score DESC) as channel_rank_by_engagement,
        ROW_NUMBER() OVER (ORDER BY messages_with_images DESC) as channel_rank_by_images
    FROM channel_stats
)

SELECT
    -- Primary key
    ROW_NUMBER() OVER (ORDER BY channel_username) as channel_key,
    
    -- Channel identifiers
    channel_username,
    channel_title,
    
    -- Message counts
    total_messages,
    medical_messages,
    messages_with_images,
    messages_with_documents,
    
    -- Engagement metrics
    avg_message_length,
    avg_engagement_score,
    total_views,
    total_forwards,
    total_replies,
    
    -- Calculated fields
    CASE 
        WHEN total_messages > 0 
        THEN ROUND((medical_messages::FLOAT / total_messages) * 100, 2)
        ELSE 0 
    END as medical_content_percentage,
    
    CASE 
        WHEN total_messages > 0 
        THEN ROUND((messages_with_images::FLOAT / total_messages) * 100, 2)
        ELSE 0 
    END as image_content_percentage,
    
    -- Rankings
    channel_rank_by_messages,
    channel_rank_by_medical_content,
    channel_rank_by_engagement,
    channel_rank_by_images,
    
    -- Channel activity period
    first_message_date,
    last_message_date,
    (last_message_date - first_message_date) as channel_age_days,
    
    -- Channel classification
    CASE 
        WHEN medical_content_percentage >= 50 THEN 'High Medical Focus'
        WHEN medical_content_percentage >= 25 THEN 'Medium Medical Focus'
        ELSE 'Low Medical Focus'
    END as medical_focus_level,
    
    CASE 
        WHEN image_content_percentage >= 50 THEN 'High Visual Content'
        WHEN image_content_percentage >= 25 THEN 'Medium Visual Content'
        ELSE 'Low Visual Content'
    END as visual_content_level,
    
    -- Timestamps
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at

FROM channel_ranking 