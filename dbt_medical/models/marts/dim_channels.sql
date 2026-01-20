{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH channel_stats AS (
    SELECT
        channel_name,
        COUNT(*) as total_posts,
        MIN(message_date) as first_post_date,
        MAX(message_date) as last_post_date,
        AVG(view_count) as avg_views,
        AVG(forward_count) as avg_forwards,
        SUM(CASE WHEN has_image_flag THEN 1 ELSE 0 END) as total_images,
        SUM(CASE WHEN has_image_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as image_percentage
    FROM {{ ref('stg_telegram_messages') }}
    GROUP BY channel_name
),

channel_classification AS (
    SELECT
        channel_name,
        CASE 
            WHEN LOWER(channel_name) LIKE '%pharm%' THEN 'Pharmaceutical'
            WHEN LOWER(channel_name) LIKE '%cosmetic%' OR LOWER(channel_name) LIKE '%beauty%' THEN 'Cosmetics'
            WHEN LOWER(channel_name) LIKE '%med%' OR LOWER(channel_name) LIKE '%health%' THEN 'Medical'
            ELSE 'Other'
        END as channel_type
    FROM channel_stats
),

final AS (
    SELECT
        {{ dbt_utils.surrogate_key(['cs.channel_name']) }} as channel_key,
        cs.channel_name,
        cc.channel_type,
        cs.first_post_date,
        cs.last_post_date,
        cs.total_posts,
        cs.avg_views,
        cs.avg_forwards,
        cs.total_images,
        cs.image_percentage,
        CASE 
            WHEN cs.last_post_date >= CURRENT_DATE - INTERVAL '7 days' THEN 'Active'
            WHEN cs.last_post_date >= CURRENT_DATE - INTERVAL '30 days' THEN 'Recently Active'
            ELSE 'Inactive'
        END as activity_status,
        CURRENT_TIMESTAMP as loaded_at
    FROM channel_stats cs
    LEFT JOIN channel_classification cc ON cs.channel_name = cc.channel_name
)

SELECT * FROM final