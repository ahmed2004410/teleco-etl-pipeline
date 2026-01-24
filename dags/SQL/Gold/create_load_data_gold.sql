-- ===================================================
-- 0. Ensure Schema Exists
-- ===================================================
CREATE SCHEMA IF NOT EXISTS gold;

-- ===================================================
-- 1. Load DimContract
-- ===================================================
--TRUNCATE TABLE gold.dim_contract CASCADE;
INSERT INTO gold.dim_contract(contract_type)
SELECT DISTINCT contract 
FROM silver.churn_raw s
WHERE contract IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM gold.dim_contract g WHERE g.contract_type = s.contract
);

-- ===================================================
-- 2. Load DimPaymentMethod
-- ===================================================
--TRUNCATE TABLE gold.dim_payment_method CASCADE;

INSERT INTO gold.dim_payment_method(payment_method)
SELECT DISTINCT payment_method 
FROM silver.churn_raw s
WHERE payment_method IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM gold.dim_payment_method g WHERE g.payment_method = s.payment_method
);

-- ===================================================
-- 3. Load DimChurnReason
-- ===================================================
--TRUNCATE TABLE gold.dim_churn_reason CASCADE;

INSERT INTO gold.dim_churn_reason(churn_reason)
SELECT DISTINCT COALESCE(churn_reason, 'n/a') 
FROM silver.churn_raw s
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_churn_reason g WHERE g.churn_reason = COALESCE(s.churn_reason, 'n/a')
);

-- ===================================================
-- 4. Load DimCustomer (Corrected)
-- ===================================================
--TRUNCATE TABLE gold.dim_customer CASCADE;
INSERT INTO gold.dim_customer (customer_id, gender, senior_citizen, partner, dependents, city, state)
SELECT DISTINCT 
    customer_id,
    gender,
    CASE WHEN senior_citizen IN ('1', 'Yes', 'True') THEN 1 ELSE 0 END,
    partner,
    dependents,
    city, 
    state
FROM silver.churn_raw s
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_customer g WHERE g.customer_id = s.customer_id
);

-- ===================================================
-- 5. Load DimServices
-- ===================================================
--TRUNCATE TABLE gold.dim_services CASCADE;
INSERT INTO gold.dim_services (
    phone_service, multiple_lines, internet_service, online_security, 
    online_backup, device_protection, tech_support, streaming_tv, streaming_movies
)
SELECT DISTINCT
    s.phone_service, s.multiple_lines, s.internet_service, s.online_security, 
    s.online_backup, s.device_protection, s.tech_support, s.streaming_tv, s.streaming_movies
FROM silver.churn_raw s
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_services g
    WHERE g.phone_service       = s.phone_service
      AND g.multiple_lines      = s.multiple_lines
      AND g.internet_service    = s.internet_service
      AND g.online_security     = s.online_security
      AND g.online_backup       = s.online_backup
      AND g.device_protection   = s.device_protection
      AND g.tech_support        = s.tech_support
      AND g.streaming_tv        = s.streaming_tv
      AND g.streaming_movies    = s.streaming_movies
);

-- ===================================================
-- 6. Load FactCustomerChurn (Smart Upsert - Postgres Style)
-- ===================================================

--DELETE FROM gold.fact_customer_churn
--WHERE run_date = '{{ ds }}';

INSERT INTO gold.fact_customer_churn (
    customer_key, contract_key, payment_method_key, churn_reason_key, service_key,
    tenure_months, monthly_charges, total_charges, churn_flag, cltv, churn_score,run_date
)
SELECT
    c.customer_key,
    ct.contract_key,
    pm.payment_method_key,
    cr.churn_reason_key,
    sv.service_key,
    
    -- 1. Tenure: حولناه لنص، نظفناه، ثم لرقم (للأمان القصوى)
    CAST(NULLIF(REGEXP_REPLACE(s.tenure_in_months::TEXT, '[^0-9.]', '', 'g'), '') AS INTEGER),
    
    s.monthly_charges_amount::DECIMAL(10,2),
    s.total_charges::DECIMAL(12,2),
    
    CASE 
      WHEN s.churn_label IN ('1','1.0','Yes','Y','True','true') THEN '1' 
      ELSE '0'
    END AS churn_flag,
    
    -- 2. CLTV: نفس الشيء، حولناه لنص أولاً
    CAST(NULLIF(REGEXP_REPLACE(s.cltv::TEXT, '[^0-9.]', '', 'g'), '') AS INTEGER),
    
    s.churn_score::DECIMAL(5,2),
    '{{ ds }}'::DATE
FROM silver.churn_raw s
JOIN gold.dim_customer c ON c.customer_id = s.customer_id

LEFT JOIN gold.dim_contract ct ON ct.contract_type = s.contract

LEFT JOIN gold.dim_payment_method pm ON pm.payment_method = s.payment_method

LEFT JOIN gold.dim_churn_reason cr ON TRIM(UPPER(cr.churn_reason)) = TRIM(UPPER(COALESCE(s.churn_reason, 'n/a')))

LEFT JOIN gold.dim_services sv
    ON sv.phone_service       = s.phone_service
   AND sv.multiple_lines      = s.multiple_lines
   AND sv.internet_service    = s.internet_service
   AND sv.online_security     = s.online_security
   AND sv.online_backup       = s.online_backup
   AND sv.device_protection   = s.device_protection
   AND sv.tech_support        = s.tech_support
   AND sv.streaming_tv        = s.streaming_tv
   AND sv.streaming_movies    = s.streaming_movies
   -- هنا الجزء المهم الجديد لمنع التكرار في الفاكت
   WHERE NOT EXISTS (
    SELECT 1 FROM gold.fact_customer_churn f
    WHERE f.customer_key::VARCHAR = c.customer_key::VARCHAR
);