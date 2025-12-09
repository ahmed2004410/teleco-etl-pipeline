from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.email import send_email
from airflow.exceptions import AirflowException
from datetime import datetime
from sqlalchemy import text
import pandas as pd
import shutil
import os
import time 
import gc
import numpy as np 

#Create necessary directories if they don't exist
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')
FIXED_DATA_PATH = os.path.join(AIRFLOW_HOME, 'include', 'fixed_data')
PROCESSED_PATH = os.path.join(AIRFLOW_HOME, 'include', 'processed_fixed_data')
REJECTED_PATH = os.path.join(AIRFLOW_HOME, 'include', 'rejected_fixes')

for path in [FIXED_DATA_PATH, PROCESSED_PATH, REJECTED_PATH]:
    os.makedirs(path, exist_ok=True)

default_args = {'owner': 'airflow'}


#---this function processes and ingests fixed data files---
def reingest_fixed_data(**kwargs):
    files = [f for f in os.listdir(FIXED_DATA_PATH) if f.endswith('.csv') or f.endswith('.xlsx')]
    
    if not files:
        print("No files found to process.")
        return

    hook = PostgresHook(postgres_conn_id='churn_db_conn')
    engine = hook.get_sqlalchemy_engine()

    # ---the checks About data Quality ---
    valid_genders = ['Male', 'Female']
    valid_internet_services = ['DSL', 'Fiber optic', 'No']
    valid_contracts = ['Month-to-month', 'One year', 'Two year']
    valid_payment_methods = ['Electronic check', 'Mailed check', 'Bank transfer (automatic)', 'Credit card (automatic)']

    print(f" Found {len(files)} files. Starting batch processing...")

    for i, file in enumerate(files):
        print(f"==================================================")
        print(f"🚀 Processing File {i+1}/{len(files)}: {file}")
        gc.collect()

        file_full_path = os.path.join(FIXED_DATA_PATH, file)

        try:
            if file.endswith('.csv'):
                df = pd.read_csv(file_full_path)
            else:
                df = pd.read_excel(file_full_path)

            df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
            column_mapping = {
                'customerid': 'customer_id',
                'tenure_months': 'tenure_in_months',
                'monthly_charges': 'monthly_charges_amount'
            }
            df.rename(columns=column_mapping, inplace=True)
            df['error_details'] = ""

            # =========================================================
            # --- (FULL DATA QUALITY CHECK) ---
            # =========================================================

            # 1. Customer ID Check
            if 'customer_id' in df.columns:
                df.loc[df['customer_id'].isnull(), 'error_details'] += "Missing ID; "
                df.loc[df.duplicated(subset=['customer_id'], keep=False), 'error_details'] += "Duplicate ID in file; "
            
            # 2. Gender Check
            if 'gender' in df.columns:
                df['gender'] = df['gender'].astype(str).str.strip().str.title()
                df.loc[~df['gender'].isin(valid_genders), 'error_details'] += "Invalid Gender; "

            # 3. Tenure Check
            if 'tenure_in_months' in df.columns:
                df['tenure_in_months'] = pd.to_numeric(df['tenure_in_months'], errors='coerce')
                df.loc[df['tenure_in_months'].isnull(), 'error_details'] += "Tenure not numeric; "
                df.loc[df['tenure_in_months'] < 0, 'error_details'] += "Negative Tenure; "

            # 4. Total Charges Check
            if 'total_charges' in df.columns:
                df['total_charges'] = pd.to_numeric(df['total_charges'], errors='coerce')
                df.loc[df['total_charges'].isnull(), 'error_details'] += "Invalid Total Charges; "

            # 5. Categorical Checks (Contract, Payment, Internet)
            if 'contract' in df.columns:
                df.loc[~df['contract'].isin(valid_contracts), 'error_details'] += "Invalid Contract Type; "
            
            if 'payment_method' in df.columns:
                df.loc[~df['payment_method'].isin(valid_payment_methods), 'error_details'] += "Invalid Payment Method; "
                
            if 'internet_service' in df.columns:
                df.loc[~df['internet_service'].isin(valid_internet_services), 'error_details'] += "Invalid Internet Service; "

            # =========================================================
            #--- clin up and separate good vs bad data ---
            # =========================================================
            bad_rows_df = df[df['error_details'] != ""]
            good_rows_df = df[df['error_details'] == ""].drop(columns=['error_details'])

            if not good_rows_df.empty:
                print(f"⏳ Processing {len(good_rows_df)} valid rows...")
                with engine.begin() as connection:
                    good_rows_df.to_sql('churn_stg', con=connection, schema='silver', if_exists='replace', index=False)
                    
                    delete_sql = """
                    DELETE FROM silver.churn_raw
                    WHERE customer_id IN (SELECT customer_id FROM silver.churn_stg);
                    """
                    connection.execute(text(delete_sql))
                    
                    insert_sql = """
                    INSERT INTO silver.churn_raw
                    SELECT * FROM silver.churn_stg;
                    """
                    connection.execute(text(insert_sql))
                print(f" Upsert Complete.")
            
            #--- bad data handling and notification ---
            if not bad_rows_df.empty:
                print(f"⚠️ Found {len(bad_rows_df)} data quality errors.")
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                clean_name = os.path.splitext(file)[0].replace('remaining_errors_', '')
                rejected_filename = f"DQ_Errors_{timestamp}_{clean_name}.xlsx"
                rejected_path = os.path.join(REJECTED_PATH, rejected_filename)
                
                bad_rows_df.to_excel(rejected_path, index=False)

                html_table = bad_rows_df[['customer_id', 'error_details']].head(20).to_html(index=False, border=1, classes='table')
                send_email(
                    to=['b4677396@gmail.com'],
                    subject=f"⚠️ DQ Alert: {file}",
                    html_content=f"<p>File: {file} contains Bad Data:</p>{html_table}",
                    files=[rejected_path]
                )
                print(" DQ Rejection email sent.")
                time.sleep(5)

            # ---move processed file to archive---
            shutil.move(file_full_path, os.path.join(PROCESSED_PATH, file))

        except Exception as e:
            print(f" CRITICAL ERROR with file {file}: {e}")
            raise AirflowException(f"Failed to process file {file}: {e}")
        
        del df
        gc.collect()

#--- Sensor function to check for files in the fixed data folder ---
def check_for_files_in_folder():
    if os.path.exists(FIXED_DATA_PATH) and len(os.listdir(FIXED_DATA_PATH)) > 0:
        return True
    return False

with DAG(
    dag_id="churn_99_reprocessing",
    description="Manually triggers when fixed data is uploaded (Idempotent + Full DQ)",
    start_date=datetime(2024, 1, 1),
    schedule=None, 
    catchup=False,
    max_active_runs=1,
    template_searchpath=[f"{AIRFLOW_HOME}/dags/SQL"], 
    tags=['churn_project', 'data_quality'],
    default_args=default_args
) as dag:

    #--- Sensor to wait for fixed data files ---
    wait_for_fixed_file = PythonSensor(
        task_id='wait_for_correction_file',
        python_callable=check_for_files_in_folder,
        poke_interval=60,
        timeout=600,
        mode='reschedule',
        soft_fail=True
    )

    #--- Task to process and ingest fixed data ---
    process_file = PythonOperator(
        task_id='process_and_ingest_fixed_data',
        python_callable=reingest_fixed_data
    )

    #--- Task to refresh Gold tables after reprocessing ---
    refresh_gold = SQLExecuteQueryOperator(
        task_id="refresh_gold_tables",
        conn_id='churn_db_conn',
        sql='Gold/create_load_data_gold.sql' 
    )

    wait_for_fixed_file >> process_file >> refresh_gold