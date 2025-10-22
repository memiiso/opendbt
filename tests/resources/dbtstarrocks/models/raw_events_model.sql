{{
    config(
        materialized='table'
    )
}}

SELECT DATE '2025-10-01' AS event_date, '{"user": "carol", "action": "login"}' AS event_payload
UNION ALL SELECT DATE '2025-09-30', '{"user": "carol", "action": "logout"}'
UNION ALL SELECT DATE '2025-09-30', '{"user": "carol", "action": "login"}'
UNION ALL SELECT DATE '2025-09-29', '{"user": "david", "action": "logout"}'
UNION ALL SELECT DATE '2025-09-28', '{"user": "david", "action": "login"}'
UNION ALL SELECT DATE '2025-09-27', '{"user": "david", "action": "logout"}'
UNION ALL SELECT DATE '2025-09-26', '{"user": "bob", "action": "login"}'
UNION ALL SELECT DATE '2025-09-26', '{"user": "bob", "action": "logout"}'
UNION ALL SELECT DATE '2025-10-06', '{"user": "lorena", "action": "login"}'
UNION ALL SELECT DATE '2025-10-07', '{"user": "lorena", "action": "logout"}'
UNION ALL SELECT DATE '2025-10-05', '{"user": "lorena", "action": "login"}'
UNION ALL SELECT DATE '2025-10-05', '{"user": "lorena", "action": "logout"}'
UNION ALL SELECT DATE '2025-10-08', '{"user": "maria", "action": "login"}'
UNION ALL SELECT DATE '2025-10-08', '{"user": "maria", "action": "logout"}'
UNION ALL SELECT DATE '2025-10-09', '{"user": "mariana", "action": "login"}'
UNION ALL SELECT DATE '2025-10-10', '{"user": "mariana", "action": "logout"}'
UNION ALL SELECT DATE '2025-10-15', '{"user": "pedro", "action": "login"}'
UNION ALL SELECT DATE '2025-10-19', '{"user": "pedro", "action": "logout"}';
