-- ============================================================
--   DATA WAREHOUSE — FULL QUALITY CHECK SCRIPT
--   Covers: Staging → Bronze → Silver → Gold + Pipeline Metadata
--   Run each section independently or all at once
-- ============================================================


-- ============================================================
-- SECTION 1: PIPELINE METADATA
-- ============================================================

-- 1.1 آخر status لكل ملف
SELECT
    file_name,
    status,
    row_count,
    file_size_bytes,
    processed_at,
    dag_run_id,
    CASE WHEN error_message IS NOT NULL THEN '❌ Has Error' ELSE '✅ Clean' END AS error_flag
FROM public.pipeline_file_metadata
ORDER BY processed_at DESC;

-- 1.2 هل في ملفات فاشلة أو لسه PROCESSING؟
SELECT status, COUNT(*) AS file_count
FROM public.pipeline_file_metadata
GROUP BY status;

-- 1.3 ملفات محتاجة إعادة معالجة (FAILED أو PROCESSING)
SELECT file_name, status, error_message, processed_at
FROM public.pipeline_file_metadata
WHERE status IN ('FAILED', 'PROCESSING')
ORDER BY processed_at DESC;


-- ============================================================
-- SECTION 2: STAGING (public.staging_churn)
-- ============================================================

-- 2.1 عدد الصفوف الكلي
SELECT COUNT(*) AS total_rows FROM public.staging_churn;

-- 2.2 فحص القيم الفارغة في الأعمدة الحيوية
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE customer_id IS NULL)             AS null_customer_id,
    COUNT(*) FILTER (WHERE gender IS NULL)                  AS null_gender,
    COUNT(*) FILTER (WHERE churn_label IS NULL)             AS null_churn_label,
    COUNT(*) FILTER (WHERE monthly_charges_amount IS NULL)  AS null_monthly_charges,
    COUNT(*) FILTER (WHERE contract IS NULL)                AS null_contract
FROM public.staging_churn;

-- 2.3 فحص القيم السالبة
SELECT COUNT(*) AS negative_values
FROM public.staging_churn
WHERE tenure_in_months < 0
   OR monthly_charges_amount < 0
   OR total_charges < 0;

-- 2.4 فحص التكرار في customer_id
SELECT customer_id, COUNT(*) AS occurrences
FROM public.staging_churn
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- 2.5 قيم gender غير صحيحة
SELECT DISTINCT gender, COUNT(*) AS cnt
FROM public.staging_churn
GROUP BY gender
ORDER BY cnt DESC;

-- 2.6 توزيع churn_label
SELECT churn_label, COUNT(*) AS cnt
FROM public.staging_churn
GROUP BY churn_label;


-- ============================================================
-- SECTION 3: BRONZE (bronze.churn_raw)
-- ============================================================

-- 3.1 عدد الصفوف الكلي
SELECT COUNT(*) AS total_rows FROM bronze.churn_raw;

-- 3.2 فحص الـ NOT NULL على الأعمدة الأساسية
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE customer_id IS NULL)   AS null_customer_id,
    COUNT(*) FILTER (WHERE churn_label IS NULL)   AS null_churn_label,
    COUNT(*) FILTER (WHERE gender IS NULL)        AS null_gender,
    COUNT(*) FILTER (WHERE country IS NULL)       AS null_country
FROM bronze.churn_raw;

-- 3.3 فحص التكرار في customer_id (يجب أن يكون 0)
SELECT customer_id, COUNT(*) AS occurrences
FROM bronze.churn_raw
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- 3.4 فحص القيم السالبة
SELECT COUNT(*) AS invalid_numeric_rows
FROM bronze.churn_raw
WHERE tenure_months < 0
   OR monthly_charges < 0
   OR total_charges < 0;

-- 3.5 نطاق القيم الرقمية
SELECT
    MIN(tenure_months)    AS min_tenure,
    MAX(tenure_months)    AS max_tenure,
    MIN(monthly_charges)  AS min_monthly,
    MAX(monthly_charges)  AS max_monthly,
    MIN(total_charges)    AS min_total,
    MAX(total_charges)    AS max_total,
    MIN(churn_score)      AS min_score,
    MAX(churn_score)      AS max_score
FROM bronze.churn_raw;

-- 3.6 فحص churn_score خارج النطاق (0–100)
SELECT COUNT(*) AS invalid_churn_score
FROM bronze.churn_raw
WHERE churn_score < 0 OR churn_score > 100;

-- 3.7 توزيع churn_label
SELECT churn_label, COUNT(*) AS cnt
FROM bronze.churn_raw
GROUP BY churn_label;


-- ============================================================
-- SECTION 4: SILVER (silver.churn_raw)
-- ============================================================

-- 4.1 عدد الصفوف
SELECT COUNT(*) AS total_rows FROM silver.churn_raw;

-- 4.2 فحص الـ NULLs
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE customer_id IS NULL)             AS null_customer_id,
    COUNT(*) FILTER (WHERE gender IS NULL)                  AS null_gender,
    COUNT(*) FILTER (WHERE churn_label IS NULL)             AS null_churn_label,
    COUNT(*) FILTER (WHERE monthly_charges_amount IS NULL)  AS null_monthly_charges,
    COUNT(*) FILTER (WHERE tenure_in_months IS NULL)        AS null_tenure,
    COUNT(*) FILTER (WHERE contract IS NULL)                AS null_contract,
    COUNT(*) FILTER (WHERE payment_method IS NULL)          AS null_payment_method
FROM silver.churn_raw;

-- 4.3 فحص القيم السالبة (يجب أن تكون 0 بعد الـ cleaning)
SELECT COUNT(*) AS invalid_rows
FROM silver.churn_raw
WHERE tenure_in_months < 0
   OR monthly_charges_amount < 0
   OR total_charges < 0;

-- 4.4 فحص gender (يجب Male / Female فقط بعد الـ cleaning)
SELECT gender, COUNT(*) AS cnt
FROM silver.churn_raw
GROUP BY gender;

-- 4.5 فحص التكرار (يجب أن يكون 0 بعد الـ cleaning)
SELECT customer_id, COUNT(*) AS occurrences
FROM silver.churn_raw
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- 4.6 نطاق القيم الرقمية
SELECT
    MIN(tenure_in_months)        AS min_tenure,
    MAX(tenure_in_months)        AS max_tenure,
    MIN(monthly_charges_amount)  AS min_monthly,
    MAX(monthly_charges_amount)  AS max_monthly,
    MIN(churn_score)             AS min_score,
    MAX(churn_score)             AS max_score
FROM silver.churn_raw;

-- 4.7 مقارنة عدد صفوف Silver مع Bronze (يجب أن يكون ≤ Bronze)
SELECT
    (SELECT COUNT(*) FROM bronze.churn_raw) AS bronze_count,
    (SELECT COUNT(*) FROM silver.churn_raw) AS silver_count,
    (SELECT COUNT(*) FROM bronze.churn_raw) - (SELECT COUNT(*) FROM silver.churn_raw) AS rows_cleaned;


-- ============================================================
-- SECTION 5: GOLD — DIMENSION TABLES
-- ============================================================

-- 5.1 عدد الصفوف في كل Dimension
SELECT 'dim_customer'      AS table_name, COUNT(*) AS row_count FROM gold.dim_customer
UNION ALL
SELECT 'dim_contract',       COUNT(*) FROM gold.dim_contract
UNION ALL
SELECT 'dim_payment_method', COUNT(*) FROM gold.dim_payment_method
UNION ALL
SELECT 'dim_services',       COUNT(*) FROM gold.dim_services
UNION ALL
SELECT 'dim_churn_reason',   COUNT(*) FROM gold.dim_churn_reason
UNION ALL
SELECT 'fact_customer_churn',COUNT(*) FROM gold.fact_customer_churn;

-- 5.2 dim_customer — فحص NULLs
SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE customer_id IS NULL) AS null_customer_id,
    COUNT(*) FILTER (WHERE gender IS NULL)      AS null_gender,
    COUNT(*) FILTER (WHERE city IS NULL)        AS null_city
FROM gold.dim_customer;

-- 5.3 dim_customer — فحص التكرار في customer_id
SELECT customer_id, COUNT(*) AS cnt
FROM gold.dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- 5.4 dim_contract — قيم contract_type الموجودة
SELECT contract_type, COUNT(*) AS cnt
FROM gold.dim_contract
GROUP BY contract_type;

-- 5.5 dim_payment_method — قيم payment_method الموجودة
SELECT payment_method, COUNT(*) AS cnt
FROM gold.dim_payment_method
GROUP BY payment_method;

-- 5.6 dim_churn_reason — هل 'n/a' موجودة (default)؟
SELECT churn_reason, COUNT(*) AS cnt
FROM gold.dim_churn_reason
GROUP BY churn_reason
ORDER BY cnt DESC;

-- 5.7 dim_services — فحص NULL في أي عمود
SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE phone_service IS NULL)    AS null_phone,
    COUNT(*) FILTER (WHERE internet_service IS NULL) AS null_internet,
    COUNT(*) FILTER (WHERE streaming_tv IS NULL)     AS null_tv
FROM gold.dim_services;


-- ============================================================
-- SECTION 6: GOLD — FACT TABLE
-- ============================================================

-- 6.1 الـ DQ Check الرئيسي (نفس check الـ DAG — يجب أن يرجع 0)
SELECT COUNT(*) AS invalid_fact_rows
FROM gold.fact_customer_churn
WHERE customer_key IS NULL
   OR contract_key IS NULL
   OR service_key IS NULL
   OR monthly_charges < 0
   OR total_charges < 0
   OR churn_score < 0
   OR churn_score > 100;

-- 6.2 Orphan Records — هل في customer_key مش موجود في dim_customer؟
SELECT COUNT(*) AS orphan_customers
FROM gold.fact_customer_churn f
LEFT JOIN gold.dim_customer c ON c.customer_key = f.customer_key
WHERE c.customer_key IS NULL;

-- 6.3 Orphan Records — هل في contract_key مش موجود في dim_contract؟
SELECT COUNT(*) AS orphan_contracts
FROM gold.fact_customer_churn f
LEFT JOIN gold.dim_contract c ON c.contract_key = f.contract_key
WHERE c.contract_key IS NULL;

-- 6.4 Orphan Records — هل في service_key مش موجود في dim_services؟
SELECT COUNT(*) AS orphan_services
FROM gold.fact_customer_churn f
LEFT JOIN gold.dim_services s ON s.service_key = f.service_key
WHERE s.service_key IS NULL;

-- 6.5 نطاق القيم الرقمية في الـ Fact
SELECT
    MIN(tenure_months)    AS min_tenure,
    MAX(tenure_months)    AS max_tenure,
    MIN(monthly_charges)  AS min_monthly,
    MAX(monthly_charges)  AS max_monthly,
    MIN(total_charges)    AS min_total,
    MAX(total_charges)    AS max_total,
    MIN(churn_score)      AS min_score,
    MAX(churn_score)      AS max_score,
    MIN(cltv)             AS min_cltv,
    MAX(cltv)             AS max_cltv
FROM gold.fact_customer_churn;

-- 6.6 توزيع churn_flag
SELECT churn_flag, COUNT(*) AS cnt
FROM gold.fact_customer_churn
GROUP BY churn_flag;

-- 6.7 فحص التكرار في customer_key (كل عميل مرة واحدة في الـ fact)
SELECT customer_key, COUNT(*) AS occurrences
FROM gold.fact_customer_churn
GROUP BY customer_key
HAVING COUNT(*) > 1;


-- ============================================================
-- SECTION 7: END-TO-END CONSISTENCY CHECKS
-- ============================================================

-- 7.1 مقارنة عدد العملاء في كل طبقة
SELECT
    (SELECT COUNT(DISTINCT customer_id) FROM bronze.churn_raw)   AS bronze_unique_customers,
    (SELECT COUNT(DISTINCT customer_id) FROM silver.churn_raw)   AS silver_unique_customers,
    (SELECT COUNT(DISTINCT customer_id) FROM gold.dim_customer)  AS gold_dim_customers,
    (SELECT COUNT(*)                    FROM gold.fact_customer_churn) AS gold_fact_rows;

-- 7.2 عملاء موجودين في Silver بس مش في Gold Fact (Missed records)
SELECT s.customer_id
FROM silver.churn_raw s
LEFT JOIN gold.dim_customer c ON c.customer_id = s.customer_id
LEFT JOIN gold.fact_customer_churn f ON f.customer_key = c.customer_key
WHERE f.fact_id IS NULL;

-- 7.3 ملخص نهائي لصحة البيانات
SELECT
    'Staging'  AS layer, COUNT(*) AS row_count FROM public.staging_churn
UNION ALL
SELECT 'Bronze',  COUNT(*) FROM bronze.churn_raw
UNION ALL
SELECT 'Silver',  COUNT(*) FROM silver.churn_raw
UNION ALL
SELECT 'Gold Fact', COUNT(*) FROM gold.fact_customer_churn;
