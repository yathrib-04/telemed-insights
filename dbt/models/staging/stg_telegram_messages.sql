{{
  config(
    materialized='view'
  )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'telegram_messages') }}
),

cleaned AS (
    SELECT
        -- Primary key
        id,
        message_id,
        channel_username,
        
        -- Channel information
        COALESCE(channel_title, 'Unknown') as channel_title,
        
        -- Message content
        COALESCE(message_text, '') as message_text,
        LENGTH(COALESCE(message_text, '')) as message_length,
        
        -- Timestamps
        message_date,
        DATE(message_date) as message_date_only,
        EXTRACT(YEAR FROM message_date) as message_year,
        EXTRACT(MONTH FROM message_date) as message_month,
        EXTRACT(DAY FROM message_date) as message_day,
        EXTRACT(DOW FROM message_date) as message_day_of_week,
        EXTRACT(HOUR FROM message_date) as message_hour,
        
        -- Media information
        has_media,
        media_type,
        media_url,
        
        -- Engagement metrics
        COALESCE(views, 0) as views,
        COALESCE(forwards, 0) as forwards,
        COALESCE(replies, 0) as replies,
        
        -- Derived fields
        CASE 
            WHEN message_text ILIKE '%medicine%' OR message_text ILIKE '%drug%' OR message_text ILIKE '%pill%' 
            OR message_text ILIKE '%tablet%' OR message_text ILIKE '%capsule%' OR message_text ILIKE '%syrup%'
            OR message_text ILIKE '%cream%' OR message_text ILIKE '%ointment%' OR message_text ILIKE '%injection%'
            OR message_text ILIKE '%vaccine%' OR message_text ILIKE '%pharmacy%' OR message_text ILIKE '%pharmaceutical%'
            THEN TRUE 
            ELSE FALSE 
        END as is_medical_related,
        
        CASE 
            WHEN has_media = TRUE AND media_type = 'photo' THEN TRUE 
            ELSE FALSE 
        END as has_image,
        
        CASE 
            WHEN has_media = TRUE AND media_type = 'document' THEN TRUE 
            ELSE FALSE 
        END as has_document,
        
        -- Engagement score (simple calculation)
        COALESCE(views, 0) + (COALESCE(forwards, 0) * 2) + (COALESCE(replies, 0) * 3) as engagement_score,
        
        -- Raw data for debugging
        raw_data,
        created_at
        
    FROM source
    WHERE message_date IS NOT NULL  -- Filter out messages without dates
)

SELECT * FROM cleaned 