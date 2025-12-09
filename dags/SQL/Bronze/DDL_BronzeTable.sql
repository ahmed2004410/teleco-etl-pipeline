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
count			  INT,
country			  VARCHAR (50),
state			  VARCHAR (50),
city			  VARCHAR (50),
zip_code	      INT,
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
churn_reason	  VARCHAR (200)
);
