{{
  config(
    materialized='view'
  )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'telegram_images') }}
),

cleaned AS (
    SELECT
        -- Primary key
        id,
        message_id,
        channel_username,
        
        -- Image information
        image_url,
        local_path,
        image_hash,
        
        -- Object detection results
        object_detection_results,
        
        -- Extract detected objects from JSON
        CASE 
            WHEN object_detection_results IS NOT NULL 
            THEN jsonb_array_length(object_detection_results)
            ELSE 0 
        END as detected_objects_count,
        
        -- Extract confidence scores
        CASE 
            WHEN object_detection_results IS NOT NULL 
            THEN (
                SELECT AVG(CAST(value->>'confidence' AS FLOAT))
                FROM jsonb_array_elements(object_detection_results) as value
            )
            ELSE NULL 
        END as avg_confidence_score,
        
        -- Extract detected classes
        CASE 
            WHEN object_detection_results IS NOT NULL 
            THEN (
                SELECT array_agg(DISTINCT value->>'class')
                FROM jsonb_array_elements(object_detection_results) as value
            )
            ELSE NULL 
        END as detected_classes,
        
        -- Check if medical objects are detected
        CASE 
            WHEN object_detection_results IS NOT NULL 
            THEN (
                SELECT COUNT(*) > 0
                FROM jsonb_array_elements(object_detection_results) as value
                WHERE value->>'class' IN ('pill', 'tablet', 'capsule', 'medicine', 'drug', 'bottle', 'syringe')
            )
            ELSE FALSE 
        END as has_medical_objects,
        
        -- Timestamps
        created_at,
        DATE(created_at) as created_date
        
    FROM source
    WHERE image_hash IS NOT NULL  -- Filter out images without hash
)

SELECT * FROM cleaned 