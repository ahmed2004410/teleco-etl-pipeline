/*
===============================================================================
Bronze Layer Loading Script (PostgreSQL - Upsert / Merge)
الهدف: نقل البيانات من Staging إلى Bronze مع الحفاظ على البيانات التاريخية وتحديث المكرر
===============================================================================
*/

-- 1. لا نستخدم TRUNCATE هنا للحفاظ على الـ Historical Data.

-- 2. إدخال البيانات أو تحديثها (Upsert)
INSERT INTO bronze.churn_raw (
    customer_id, gender, senior_citizen, partner, dependents, country, state, city, zip_code, 
    lat_long, latitude, longitude, phone_service, multiple_lines, internet_service, online_security, 
    online_backup, device_protection, tech_support, streaming_tv, streaming_movies, paperless_billing, 
    payment_method, contract, tenure_months, monthly_charges, total_charges, churn_label, churn_value, 
    churn_score, cltv, churn_reason, created_at, updated_at, record_type
)
SELECT 
    customer_id,
    gender,
    senior_citizen,
    partner,
    dependents,
    country,
    state,
    city,
    -- ✅ Safe Casting: يحول الفراغات لـ NULL بدل ما يضرب Error
    CAST(NULLIF(TRIM(zip_code::TEXT), '') AS INTEGER), 
    
    -- ✅ Fix: دمج خط الطول والعرض لإنشاء lat_long لو مش موجود في Source
    CAST(latitude AS TEXT) || ',' || CAST(longitude AS TEXT) AS lat_long, 
    
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
    churn_reason,
    CURRENT_TIMESTAMP, -- created_at
    CURRENT_TIMESTAMP, -- updated_at
    'upserted'
FROM public.staging_churn

ON CONFLICT (customer_id) 
DO UPDATE SET 
    gender = EXCLUDED.gender,
    senior_citizen = EXCLUDED.senior_citizen,
    partner = EXCLUDED.partner,
    dependents = EXCLUDED.dependents,
    state = EXCLUDED.state,
    city = EXCLUDED.city,
    zip_code = EXCLUDED.zip_code,
    lat_long = EXCLUDED.lat_long,
    contract = EXCLUDED.contract,
    tenure_months = EXCLUDED.tenure_months,
    monthly_charges = EXCLUDED.monthly_charges,
    total_charges = EXCLUDED.total_charges,
    churn_label = EXCLUDED.churn_label,
    churn_value = EXCLUDED.churn_value,
    updated_at = CURRENT_TIMESTAMP, -- تحديث وقت التعديل
    record_type = 'updated';