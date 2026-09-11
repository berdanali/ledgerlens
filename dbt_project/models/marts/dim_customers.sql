-- Grain: one row per customer — current state only (SCD Type 1).
-- Intentional choice: customers is the highest-PII table; Type 1 limits
-- how much historical personal data we retain, which simplifies GDPR
-- right-to-erasure compliance (one row to purge per customer).

select
    customer_id,
    first_name_token,
    last_name_token,
    email_token,
    national_id_token,
    date_of_birth_token,
    created_at,
    updated_at
from {{ ref('stg_customers') }}
