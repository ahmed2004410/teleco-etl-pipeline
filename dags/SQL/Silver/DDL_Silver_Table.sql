CREATE SCHEMA IF NOT EXISTS silver;

Create TABLE IF NOT EXISTS silver.churn_raw

 (
    customer_id VARCHAR(50),
    gender VARCHAR(10),
    senior_citizen VARCHAR(5),    -- تم التعديل من Senior Citizen
    partner VARCHAR(5),
    dependents VARCHAR(5),
    count INTEGER,
    country VARCHAR(50),
    state VARCHAR(50),
    city VARCHAR(50),
    zip_code INTEGER,
    lat_long VARCHAR(100),
    latitude NUMERIC,
    longitude NUMERIC,
    phone_service VARCHAR(5),
    multiple_lines VARCHAR(50),
    internet_service VARCHAR(50),
    online_security VARCHAR(50),
    online_backup VARCHAR(50),
    device_protection VARCHAR(50),
    tech_support VARCHAR(50),
    streaming_tv VARCHAR(50),
    streaming_movies VARCHAR(50),
    paperless_billing VARCHAR(5),
    payment_method VARCHAR(50),
    contract VARCHAR(50),
    tenure_in_months INTEGER,      -- تأكد من هذا الاسم (أو tenure_months حسب توحيدك)
    monthly_charges_amount NUMERIC,
    total_charges NUMERIC,
    churn_label VARCHAR(5),
    churn_value INTEGER,
    churn_score INTEGER,
    cltv INTEGER,
    churn_reason VARCHAR(100)
);