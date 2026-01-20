-- Test to ensure message length is reasonable (less than 10,000 characters)
SELECT 
    COUNT(*) as long_messages_count
FROM {{ ref('stg_telegram_messages') }}
WHERE message_length > 10000

-- This test passes if it returns 0 rows
HAVING COUNT(*) > 0