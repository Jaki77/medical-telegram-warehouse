{{
    config(
        materialized='view',
        schema='staging'
    )
}}

WITH raw_messages AS (
    SELECT
        message_id,
        channel_name,
        message_date,
        message_text,
        has_media,
        image_path,
        views,
        forwards,
        scraped_at,
        raw_data
    FROM {{ source('raw', 'telegram_messages') }}
),

cleaned_messages AS (
    SELECT
        -- Primary key
        {{ dbt_utils.surrogate_key(['message_id', 'channel_name']) }} as message_key,
        
        -- Message identifiers
        message_id,
        channel_name,
        
        -- Dates
        message_date,
        scraped_at,
        DATE(message_date) as message_date_only,
        
        -- Text content
        TRIM(message_text) as message_text,
        LENGTH(TRIM(message_text)) as message_length,
        
        -- Media information
        has_media,
        image_path,
        CASE 
            WHEN image_path IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END as has_image_flag,
        
        -- Engagement metrics
        COALESCE(views, 0) as view_count,
        COALESCE(forwards, 0) as forward_count,
        
        -- Quality flags
        CASE 
            WHEN message_text IS NULL OR message_text = '' THEN TRUE
            ELSE FALSE
        END as is_empty_message,
        
        CASE 
            WHEN message_date > CURRENT_TIMESTAMP THEN TRUE
            ELSE FALSE
        END as is_future_date,
        
        -- Extract product mentions (simple pattern matching)
        CASE 
            WHEN LOWER(message_text) LIKE '%paracetamol%' OR LOWER(message_text) LIKE '%panadol%' THEN 'Paracetamol'
            WHEN LOWER(message_text) LIKE '%amoxicillin%' OR LOWER(message_text) LIKE '%amoxil%' THEN 'Amoxicillin'
            WHEN LOWER(message_text) LIKE '%vitamin%' THEN 'Vitamin'
            WHEN LOWER(message_text) LIKE '%cream%' THEN 'Cream'
            WHEN LOWER(message_text) LIKE '%pill%' OR LOWER(message_text) LIKE '%tablet%' THEN 'Pill/Tablet'
            ELSE 'Other'
        END as detected_product_category,
        
        -- Raw data for debugging
        raw_data
        
    FROM raw_messages
),

final AS (
    SELECT
        message_key,
        message_id,
        channel_name,
        message_date,
        scraped_at,
        message_date_only,
        message_text,
        message_length,
        has_media,
        has_image_flag,
        image_path,
        view_count,
        forward_count,
        is_empty_message,
        is_future_date,
        detected_product_category,
        raw_data,
        -- Add metadata
        CURRENT_TIMESTAMP as transformed_at
    FROM cleaned_messages
    -- Filter out invalid records (optional, based on business rules)
    WHERE NOT is_empty_message
      AND NOT is_future_date
)

SELECT * FROM final