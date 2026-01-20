-- Test to ensure view counts are non-negative
SELECT 
    COUNT(*) as negative_views_count
FROM {{ ref('stg_telegram_messages') }}
WHERE view_count < 0

-- This test passes if it returns 0 rows
HAVING COUNT(*) > 0