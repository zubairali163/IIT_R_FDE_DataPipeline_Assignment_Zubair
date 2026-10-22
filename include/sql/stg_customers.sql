CREATE OR REPLACE TABLE stg_customers AS
WITH a AS (
    SELECT *, rowid AS _rid FROM raw_customers
),
latest AS (
    SELECT * FROM a
    QUALIFY row_number() OVER (PARTITION BY trim(customer_id) ORDER BY _loaded_at DESC, _rid) = 1
),
clean AS (
    SELECT trim(customer_id) AS customer_id,
           trim(full_name) AS full_name,
           trim(email) AS email,
           trim(phone) AS phone,
           trim(city) AS city,
           trim(area) AS area,
           trim(pincode) AS pincode,
           try_cast(trim(signup_date) AS DATE) AS signup_date,
           upper(left(trim(loyalty_tier), 1)) || lower(substr(trim(loyalty_tier), 2)) AS loyalty_tier,
           trim(is_active) AS is_active,
           _run_id, _source_file
    FROM latest
),
flagged AS (
    SELECT *,
        concat_ws('; ',
            CASE WHEN signup_date > current_date THEN 'signup_not_future' END) AS failure_reason,
        concat_ws('; ',
            --CASE WHEN NOT regexp_matches(email, '^[^@\s]+@[^@\s]+\.[^@\s]+$') THEN 'email_format' END,
            --CASE WHEN NOT regexp_matches(phone, '^[0-9]{10}$') THEN 'phone_10_digits' END,
            --CASE WHEN NOT regexp_matches(pincode, '^[0-9]{6}$') THEN 'pincode_6_digits' END,
            CASE WHEN NOT regexp_matches(coalesce(email, ''), '^[^@\s]+@[^@\s]+\.[^@\s]+$') THEN 'email_format' END,
            CASE WHEN NOT regexp_matches(coalesce(phone, ''), '^[0-9]{10}$') THEN 'phone_10_digits' END,
            CASE WHEN NOT regexp_matches(coalesce(pincode, ''), '^[0-9]{6}$') THEN 'pincode_6_digits' END,
            CASE WHEN city IS NULL OR city = '' THEN 'city_not_null' END,
            CASE WHEN loyalty_tier NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum') THEN 'tier_allowed' END
        ) AS warning_reason
    FROM clean
)
SELECT customer_id, full_name,
       CASE WHEN regexp_matches(email, '^[^@\s]+@[^@\s]+\.[^@\s]+$')
            THEN substr(email, 1, 2) || '***@' || split_part(email, '@', 2) ELSE 'INVALID' END AS email_masked,
       CASE WHEN regexp_matches(phone, '^[0-9]{10}$')
            THEN substr(phone, 1, 2) || '******' || substr(phone, 9, 2) ELSE 'INVALID' END AS phone_masked,
       city, area, pincode, signup_date, loyalty_tier, is_active,
       CASE WHEN failure_reason <> '' THEN 'FAIL'
            WHEN warning_reason <> '' THEN 'WARN' ELSE 'PASS' END AS dq_status,
       failure_reason, warning_reason, _run_id, _source_file
FROM flagged;