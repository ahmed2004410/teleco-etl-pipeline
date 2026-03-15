/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
*/
CREATE SCHEMA IF NOT EXISTS bronze;

Create table IF NOT EXISTS bronze.churn_raw
(
customer_id		  VARCHAR (100),
gender		      VARCHAR (10),
senior_citizen    VARCHAR (20),
partner			  VARCHAR (20),
dependents		  VARCHAR (20),
country			  VARCHAR (50),
state			  VARCHAR (50),
city			  VARCHAR (50),
zip_code	      INTEGER,
lat_long	      VARCHAR (50),
latitude	      FLOAT,
longitude	      FLOAT,
phone_service	  VARCHAR (20),
multiple_lines	  VARCHAR (50),
internet_service  VARCHAR (50),
online_security   VARCHAR (50),
online_backup	  VARCHAR (50),
device_protection VARCHAR (50),
tech_support	  VARCHAR (50),
streaming_tv	  VARCHAR (50),
streaming_movies  VARCHAR (50),
paperless_billing VARCHAR (20),
payment_method	  VARCHAR (50),
contract		  VARCHAR (50),
tenure_months	  INT,
monthly_charges	  FLOAT,
total_charges	  FLOAT,
churn_label		  VARCHAR (20),
churn_value		  INT,
churn_score		  INT,
cltv			  INT,
churn_reason	  VARCHAR (200),
created_at        TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP,
updated_at        TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP,
record_type       VARCHAR (20)     NOT NULL DEFAULT 'upserted',
UNIQUE (customer_id)
);

CREATE TABLE IF NOT EXISTS bronze.processed_files (
    file_name TEXT PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT
);


CREATE TABLE IF NOT EXISTS public.staging_churn (
    customer_id             VARCHAR(255),
    gender                  VARCHAR(50),
    senior_citizen          VARCHAR(30),
    partner                 VARCHAR(30),
    dependents              VARCHAR(30),
    country                 VARCHAR(100),
    state                   VARCHAR(100),
    city                    VARCHAR(100),
    zip_code                INTEGER,
    lat_long                VARCHAR(100),
    latitude                FLOAT,
    longitude               FLOAT,
    phone_service           VARCHAR(30),
    multiple_lines          VARCHAR(50),
    internet_service        VARCHAR(50),
    online_security         VARCHAR(30),
    online_backup           VARCHAR(30),
    device_protection       VARCHAR(30),
    tech_support            VARCHAR(30),
    streaming_tv            VARCHAR(30),
    streaming_movies        VARCHAR(30),
    paperless_billing       VARCHAR(30),
    payment_method          VARCHAR(100),
    contract                VARCHAR(50),
    tenure_in_months        FLOAT,
    monthly_charges_amount  FLOAT,
    total_charges           FLOAT,
    churn_label             VARCHAR(10),
    churn_value             INTEGER,
    churn_score             INTEGER,
    cltv                    INTEGER,
    churn_reason            VARCHAR(255)
);