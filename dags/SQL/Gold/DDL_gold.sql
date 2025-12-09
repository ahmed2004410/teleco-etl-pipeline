CREATE SCHEMA IF NOT EXISTS gold;

-- 1. جدول العقود
CREATE TABLE IF NOT EXISTS gold.dim_contract (
    contract_key SERIAL PRIMARY KEY,
    contract_type VARCHAR(255)
);

-- 2. جدول طرق الدفع
CREATE TABLE IF NOT EXISTS gold.dim_payment_method (
    payment_method_key SERIAL PRIMARY KEY,
    payment_method VARCHAR(255)
);

-- 3. جدول أسباب الإلغاء
CREATE TABLE IF NOT EXISTS gold.dim_churn_reason (
    churn_reason_key SERIAL PRIMARY KEY,
    churn_reason VARCHAR(255)
);

-- 4. جدول العملاء
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    gender VARCHAR(50),
    senior_citizen VARCHAR(20),
    partner VARCHAR(50),
    dependents VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(100)
);

-- 5. جدول الخدمات
CREATE TABLE IF NOT EXISTS gold.dim_services (
    service_key SERIAL PRIMARY KEY,
    phone_service VARCHAR(50),
    multiple_lines VARCHAR(50),
    internet_service VARCHAR(50),
    online_security VARCHAR(50),
    online_backup VARCHAR(50),
    device_protection VARCHAR(50),
    tech_support VARCHAR(50),
    streaming_tv VARCHAR(50),
    streaming_movies VARCHAR(50)
);

-- 6. جدول الحقائق
CREATE TABLE IF NOT EXISTS gold.fact_customer_churn (
    fact_id SERIAL PRIMARY KEY,
    customer_key VARCHAR(50),
    contract_key INT,
    payment_method_key INT,
    churn_reason_key INT,
    service_key INT,
    tenure_months INT,
    monthly_charges DECIMAL(10,2),
    total_charges DECIMAL(12,2),
    churn_flag VARCHAR(10),
    cltv DECIMAL(12,2),
    churn_score DECIMAL(5,2)
);