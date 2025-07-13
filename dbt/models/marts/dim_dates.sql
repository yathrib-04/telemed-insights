{{
  config(
    materialized='table'
  )
}}

WITH date_spine AS (
    SELECT 
        date_actual,
        date_actual as date_key,
        EXTRACT(YEAR FROM date_actual) as year,
        EXTRACT(MONTH FROM date_actual) as month,
        EXTRACT(DAY FROM date_actual) as day,
        EXTRACT(DOW FROM date_actual) as day_of_week,
        EXTRACT(DOY FROM date_actual) as day_of_year,
        EXTRACT(WEEK FROM date_actual) as week_of_year,
        EXTRACT(QUARTER FROM date_actual) as quarter,
        
        -- Month names
        TO_CHAR(date_actual, 'Month') as month_name,
        TO_CHAR(date_actual, 'Mon') as month_name_short,
        
        -- Day names
        TO_CHAR(date_actual, 'Day') as day_name,
        TO_CHAR(date_actual, 'Dy') as day_name_short,
        
        -- Fiscal year (assuming fiscal year starts in July)
        CASE 
            WHEN EXTRACT(MONTH FROM date_actual) >= 7 
            THEN EXTRACT(YEAR FROM date_actual) + 1
            ELSE EXTRACT(YEAR FROM date_actual)
        END as fiscal_year,
        
        -- Fiscal quarter
        CASE 
            WHEN EXTRACT(MONTH FROM date_actual) IN (7, 8, 9) THEN 1
            WHEN EXTRACT(MONTH FROM date_actual) IN (10, 11, 12) THEN 2
            WHEN EXTRACT(MONTH FROM date_actual) IN (1, 2, 3) THEN 3
            WHEN EXTRACT(MONTH FROM date_actual) IN (4, 5, 6) THEN 4
        END as fiscal_quarter,
        
        -- Weekday vs weekend
        CASE 
            WHEN EXTRACT(DOW FROM date_actual) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END as day_type,
        
        -- Season
        CASE 
            WHEN EXTRACT(MONTH FROM date_actual) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM date_actual) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM date_actual) IN (6, 7, 8) THEN 'Summer'
            WHEN EXTRACT(MONTH FROM date_actual) IN (9, 10, 11) THEN 'Fall'
        END as season,
        
        -- Ethiopian calendar (approximate conversion)
        EXTRACT(YEAR FROM date_actual) - 7 as ethiopian_year,
        
        -- Business day (excluding weekends for now)
        CASE 
            WHEN EXTRACT(DOW FROM date_actual) IN (0, 6) THEN FALSE
            ELSE TRUE
        END as is_business_day
        
    FROM (
        SELECT generate_series(
            '2023-01-01'::date,
            '2025-12-31'::date,
            '1 day'::interval
        )::date as date_actual
    ) as date_spine
),

date_metrics AS (
    SELECT
        d.*,
        -- Add message counts from staging
        COALESCE(msg.message_count, 0) as message_count,
        COALESCE(msg.medical_message_count, 0) as medical_message_count,
        COALESCE(msg.image_message_count, 0) as image_message_count,
        COALESCE(msg.avg_engagement_score, 0) as avg_engagement_score
        
    FROM date_spine d
    LEFT JOIN (
        SELECT
            DATE(message_date) as date_actual,
            COUNT(*) as message_count,
            COUNT(CASE WHEN is_medical_related THEN 1 END) as medical_message_count,
            COUNT(CASE WHEN has_image THEN 1 END) as image_message_count,
            AVG(engagement_score) as avg_engagement_score
        FROM {{ ref('stg_telegram_messages') }}
        GROUP BY DATE(message_date)
    ) msg ON d.date_actual = msg.date_actual
)

SELECT
    date_key,
    date_actual,
    year,
    month,
    day,
    day_of_week,
    day_of_year,
    week_of_year,
    quarter,
    month_name,
    month_name_short,
    day_name,
    day_name_short,
    fiscal_year,
    fiscal_quarter,
    day_type,
    season,
    ethiopian_year,
    is_business_day,
    message_count,
    medical_message_count,
    image_message_count,
    avg_engagement_score,
    
    -- Rolling metrics (7-day)
    AVG(message_count) OVER (
        ORDER BY date_actual 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as message_count_7d_avg,
    
    AVG(medical_message_count) OVER (
        ORDER BY date_actual 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as medical_message_count_7d_avg,
    
    -- Rolling metrics (30-day)
    AVG(message_count) OVER (
        ORDER BY date_actual 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as message_count_30d_avg,
    
    -- Medical content percentage
    CASE 
        WHEN message_count > 0 
        THEN ROUND((medical_message_count::FLOAT / message_count) * 100, 2)
        ELSE 0 
    END as medical_content_percentage,
    
    -- Image content percentage
    CASE 
        WHEN message_count > 0 
        THEN ROUND((image_message_count::FLOAT / message_count) * 100, 2)
        ELSE 0 
    END as image_content_percentage

FROM date_metrics
ORDER BY date_actual 