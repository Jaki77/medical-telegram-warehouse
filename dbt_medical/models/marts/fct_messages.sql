{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH messages AS (
    SELECT
        m.message_key,
        m.message_id,
        m.channel_name,
        m.message_date,
        m.message_text,
        m.message_length,
        m.view_count,
        m.forward_count,
        m.has_image_flag,
        m.detected_product_category,
        dc.channel_key,
        dd.date_key
    FROM {{ ref('stg_telegram_messages') }} m
    LEFT JOIN {{ ref('dim_channels') }} dc ON m.channel_name = dc.channel_name
    LEFT JOIN {{ ref('dim_dates') }} dd ON DATE(m.message_date) = dd.full_date
),

final AS (
    SELECT
        message_key,
        message_id,
        channel_key,
        date_key,
        message_text,
        message_length,
        view_count,
        forward_count,
        has_image_flag,
        detected_product_category,
        -- Engagement score (simple metric)
        view_count * 0.7 + forward_count * 0.3 as engagement_score,
        -- Message type classification
        CASE 
            WHEN message_length < 50 THEN 'Short'
            WHEN message_length BETWEEN 50 AND 200 THEN 'Medium'
            ELSE 'Long'
        END as message_length_category,
        CURRENT_TIMESTAMP as loaded_at
    FROM messages
)

SELECT * FROM final