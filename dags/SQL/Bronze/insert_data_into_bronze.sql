
/*
===============================================================================
Bronze Layer Loading Script (Postgres Version)
الهدف: نقل البيانات من الجدول المؤقت (staging) إلى الجدول البرونزي
===============================================================================
*/

-- 1. تنظيف الجدول البرونزي (بدلاً من Truncate التقليدية)

TRUNCATE TABLE bronze.churn_raw;

-- 2. إدخال البيانات (مع معالجة lat_long في نفس الخطوة)
INSERT INTO bronze.churn_raw (
    customer_id,
    gender,
    senior_citizen,
    partner,
    dependents,
    count,
    country,
    state,
    city,
    zip_code,
    lat_long,
    latitude,
    longitude,
    phone_service,
    multiple_lines,
    internet_service,
    online_security,
    online_backup,
    device_protection,
    tech_support,
    streaming_tv,
    streaming_movies,
    paperless_billing,
    payment_method,
    contract,
    tenure_months,
    monthly_charges,
    total_charges,
    churn_label,
    churn_value,
    churn_score,
    cltv,
    churn_reason
)
SELECT 
    customer_id,             -- الاسم الجديد (بدلاً من "CustomerID")
    gender,
    senior_citizen,          -- الاسم الجديد (بدلاً من "Senior Citizen")
    partner,
    dependents,
    count,
    country,
    state,
    city,
    zip_code,
    REPLACE(lat_long, '&', ','),
    latitude,
    longitude,
    phone_service,
    multiple_lines,
    internet_service,
    online_security,
    online_backup,
    device_protection,
    tech_support,
    streaming_tv,
    streaming_movies,
    paperless_billing,
    payment_method,
    contract,
    tenure_in_months,       
    monthly_charges_amount,
    total_charges,
    churn_label,
    churn_value,
    churn_score,
    cltv,
    churn_reason
FROM public.staging_churn;