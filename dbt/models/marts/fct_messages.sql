{{
  config(
    materialized='table'
  )
}}

WITH messages_with_dimensions AS (
    SELECT
        -- Fact table primary key
        m.id as message_key,
        
        -- Foreign keys to dimension tables
        c.channel_key,
        d.date_key,
        
        -- Message identifiers
        m.message_id,
        m.channel_username,
        m.channel_title,
        
        -- Message content
        m.message_text,
        m.message_length,
        
        -- Timestamps
        m.message_date,
        m.message_date_only,
        m.message_year,
        m.message_month,
        m.message_day,
        m.message_day_of_week,
        m.message_hour,
        
        -- Media information
        m.has_media,
        m.media_type,
        m.media_url,
        m.has_image,
        m.has_document,
        
        -- Engagement metrics
        m.views,
        m.forwards,
        m.replies,
        m.engagement_score,
        
        -- Content classification
        m.is_medical_related,
        
        -- Image analysis (if available)
        img.detected_objects_count,
        img.avg_confidence_score,
        img.detected_classes,
        img.has_medical_objects,
        
        -- Dimension table attributes for easier querying
        c.medical_focus_level,
        c.visual_content_level,
        c.channel_rank_by_messages,
        c.channel_rank_by_medical_content,
        c.channel_rank_by_engagement,
        
        d.day_type,
        d.season,
        d.is_business_day,
        d.medical_content_percentage as date_medical_content_percentage,
        d.image_content_percentage as date_image_content_percentage,
        
        -- Raw data for debugging
        m.raw_data,
        m.created_at
        
    FROM {{ ref('stg_telegram_messages') }} m
    LEFT JOIN {{ ref('dim_channels') }} c 
        ON m.channel_username = c.channel_username
    LEFT JOIN {{ ref('dim_dates') }} d 
        ON m.message_date_only = d.date_actual
    LEFT JOIN {{ ref('stg_telegram_images') }} img 
        ON m.message_id = img.message_id 
        AND m.channel_username = img.channel_username
)

SELECT
    message_key,
    channel_key,
    date_key,
    message_id,
    channel_username,
    channel_title,
    message_text,
    message_length,
    message_date,
    message_date_only,
    message_year,
    message_month,
    message_day,
    message_day_of_week,
    message_hour,
    has_media,
    media_type,
    media_url,
    has_image,
    has_document,
    views,
    forwards,
    replies,
    engagement_score,
    is_medical_related,
    detected_objects_count,
    avg_confidence_score,
    detected_classes,
    has_medical_objects,
    medical_focus_level,
    visual_content_level,
    channel_rank_by_messages,
    channel_rank_by_medical_content,
    channel_rank_by_engagement,
    day_type,
    season,
    is_business_day,
    date_medical_content_percentage,
    date_image_content_percentage,
    raw_data,
    created_at,
    
    -- Additional calculated fields
    CASE 
        WHEN has_image AND has_medical_objects THEN 'Medical Image'
        WHEN has_image THEN 'Non-Medical Image'
        WHEN has_document THEN 'Document'
        WHEN is_medical_related THEN 'Medical Text'
        ELSE 'Other'
    END as content_category,
    
    CASE 
        WHEN engagement_score >= 100 THEN 'High Engagement'
        WHEN engagement_score >= 50 THEN 'Medium Engagement'
        WHEN engagement_score >= 10 THEN 'Low Engagement'
        ELSE 'No Engagement'
    END as engagement_level,
    
    -- Time-based classifications
    CASE 
        WHEN message_hour BETWEEN 6 AND 12 THEN 'Morning'
        WHEN message_hour BETWEEN 12 AND 18 THEN 'Afternoon'
        WHEN message_hour BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Night'
    END as time_of_day,
    
    -- Content quality score
    (
        CASE WHEN message_length > 0 THEN 1 ELSE 0 END +
        CASE WHEN has_media THEN 2 ELSE 0 END +
        CASE WHEN is_medical_related THEN 3 ELSE 0 END +
        CASE WHEN has_medical_objects THEN 2 ELSE 0 END +
        CASE WHEN engagement_score > 0 THEN 1 ELSE 0 END
    ) as content_quality_score

FROM messages_with_dimensions 