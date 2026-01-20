-- Test to ensure no messages have future dates
SELECT 
    COUNT(*) as future_messages_count
FROM {{ ref('stg_telegram_messages') }}
WHERE message_date > CURRENT_TIMESTAMP

-- This test passes if it returns 0 rows
HAVING COUNT(*) > 0