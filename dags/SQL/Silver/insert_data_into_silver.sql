--CREATE OR REPLACE PROCEDURE silver_load_data()
--LANGUAGE plpgsql
--AS $$
--BEGIN
    -- 1. مسح البيانات القديمة لضمان عدم التكرار
    TRUNCATE TABLE silver.churn_raw;

    -- 2. إدخال البيانات (تم تعديل الأسماء لتطابق الجدول)
    INSERT INTO silver.churn_raw (
        customer_id,
        gender,
        senior_citizen,        -- كان is_senior_citizen
        partner,               -- كان has_partner
        dependents,            -- كان has_dependents
        count,
        country,
        state,
        city,
        zip_code,
        lat_long,
        latitude,
        longitude,
        phone_service,         -- كان has_phone_service
        multiple_lines,        -- كان has_multiple_lines
        internet_service,      -- كان internet_service_type
        online_security,       -- كان has_online_security
        online_backup,         -- كان has_online_backup
        device_protection,     -- كان has_device_protection
        tech_support,          -- كان has_tech_support
        streaming_tv,          -- كان has_streaming_tv
        streaming_movies,      -- كان has_streaming_movies
        paperless_billing,     -- كان is_paperless_billing
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
    )
    SELECT 
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
        COALESCE(churn_reason, 'n/a')
    FROM bronze.churn_raw;
END;
--$$;