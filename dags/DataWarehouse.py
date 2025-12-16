from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator, SQLValueCheckOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.utils.email import send_email 
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from airflow import DAG
import pandas as pd
import shutil 
import glob
import os 
import re 

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')
ARCHIVE_PATH = os.path.join(AIRFLOW_HOME, 'include', 'archive')
os.makedirs(ARCHIVE_PATH, exist_ok=True)

STAGING_PATH = os.path.join(AIRFLOW_HOME, 'include', 'staging')
QUARANTINE_PATH = os.path.join(AIRFLOW_HOME, 'include', 'quarantine')
REPORTS_PATH = os.path.join(AIRFLOW_HOME, 'include', 'reports')

for path in [STAGING_PATH, QUARANTINE_PATH, REPORTS_PATH]:
    os.makedirs(path, exist_ok=True)

#======== to make sure smtp connection is working ===================#
def debug_smtp_connection():
    try:
        conn = BaseHook.get_connection("smtp_default")
        print(f" Detective Airflow Report:")
        print(f"   -> Host detected: {conn.host}")
        print(f"   -> Login detected: {conn.login}")
        print(f"   -> Port detected: {conn.port}")
    except Exception as e:
        print(f" Could not find smtp_default connection: {e}")


#================ send email on task failure ========================#
def notify_email_on_failure(context):
    try:
        task_instance = context['task_instance']
        exception = context.get('exception') 
        error_message = str(exception) if exception else "Unknown Error"
        
        print(" Attempting to send failure email manually...")
        send_email(
            to=['b4677396@gmail.com'],
            subject=f"🚨 FAILED: {task_instance.task_id}",
            html_content=f"""
            <h3>Something went wrong!</h3>
            <p>Task: <b>{task_instance.task_id}</b> failed.</p>
            <p style="color:red; font-size:16px;"><b>Error Details: {error_message}</b></p> <p>DAG: {task_instance.dag_id}</p>
            <p>Time: {datetime.now()}</p>
            """
        )
        print(" Email sent successfully.")
    except Exception as e:
        print(f" Failed to send email: {e}")

default_args = {
    'owner': 'airflow',
    'on_failure_callback': notify_email_on_failure
    }

#================= Modified: Isolate bad rows, export to Excel, then clean ===================#
def clean_or_stop_silver(**kwargs):
    hook = PostgresHook(postgres_conn_id='churn_db_conn')
    
    #---get bad data from silver table based on the defined rules ---#
    fetch_bad_data_sql = """
        SELECT * FROM silver.churn_raw
        WHERE customer_id IS NULL 
           OR tenure_in_months < 0 
           OR monthly_charges_amount < 0 
           OR gender NOT IN ('Male', 'Female')
           OR customer_id IN (
               SELECT customer_id 
               FROM silver.churn_raw 
               GROUP BY customer_id 
               HAVING COUNT(*) > 1
           );
    """
    df_bad = hook.get_pandas_df(fetch_bad_data_sql)
    
    #---check if there are any bad rows ---#
    if df_bad.empty:
        print("Data is clean. No errors found.")
        return

    #---count total bad rows ---#
    total_bad_rows = len(df_bad)
    print(f"Found {total_bad_rows} problematic rows. Analyzing errors...")

    #--analyze errors in bad rows --#
    df_bad['error_details'] = ""

    #check individual error conditions ---#
    df_bad.loc[df_bad['customer_id'].isnull(), 'error_details'] += "Missing ID; "
    df_bad.loc[df_bad['tenure_in_months'] < 0, 'error_details'] += "Negative Tenure; "
    df_bad.loc[df_bad['monthly_charges_amount'] < 0, 'error_details'] += "Negative Charges; "
    df_bad.loc[~df_bad['gender'].isin(['Male', 'Female']), 'error_details'] += "Invalid Gender; "

    #---check for duplicate customer_ids ---#
    duplicated_ids = df_bad[df_bad.duplicated(subset=['customer_id'], keep=False)]['customer_id'].unique()
    df_bad.loc[df_bad['customer_id'].isin(duplicated_ids), 'error_details'] += "Duplicate ID; "

    #---clean up error details formatting ---#
    df_bad['error_details'] = df_bad['error_details'].str.strip('; ')

    #---decide action based on error count ---#
    ERROR_THRESHOLD = 50

    if total_bad_rows > ERROR_THRESHOLD:

        #--Stop Pipeline if errors exceed threshold --#
        print(f"🚨 CRITICAL: Error count ({total_bad_rows}) exceeded threshold!")
        
        send_email(
            to=['b4677396@gmail.com'],
            subject=f"🚨 PIPELINE STOPPED: High Error Rate ({total_bad_rows})",
            html_content=f"""
            <h3>Critical Data Quality Issue</h3>
            <p>The pipeline stopped because <b>{total_bad_rows}</b> bad rows were found (Threshold: {ERROR_THRESHOLD}).</p>
            <p>Please check the database table <i>silver.churn_raw</i> immediately.</p>
            """
        )
        raise AirflowException(f"Pipeline stopped. Too many errors ({total_bad_rows}).")

    else:
        # (Isolate & Clean)
        print(f" Errors within limit. Isolating {total_bad_rows} rows to Quarantine...")

        #--- a. Export bad rows to Excel in Quarantine folder ---#
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"quarantine_bronze_output_{current_time}.xlsx"
        file_path = os.path.join(QUARANTINE_PATH, file_name)
        
        #---export to excel with a new coulmn for error details ---#
        df_bad.to_excel(file_path, index=False)
        print(f" Data saved to: {file_path}")

        #--- b. Send email notification with summary and attachment ---#
        error_rows_html = ""
        for index, row in df_bad.head(20).iterrows():
            excel_row_num = index + 2
            reason = row['error_details']
            cid = row['customer_id']
            error_rows_html += f"<tr><td>{excel_row_num}</td><td>{cid}</td><td style='color:red;'>{reason}</td></tr>"

        if total_bad_rows > 20:
            error_rows_html += f"<tr><td colspan='3'>... and {total_bad_rows - 20} more.</td></tr>"

        email_body = f"""
        <h3 style="color:orange;"> Data Quality Alert (Auto-Cleaned)</h3>
        <p>Found <b>{total_bad_rows}</b> bad rows during Silver processing.</p>
        <p><b>Action Taken:</b> These rows were moved to Quarantine (Attached) and deleted from the database so the pipeline can continue.</p>
        <br>
        <table border="1" cellpadding="5" style="border-collapse: collapse;">
            <tr style="background-color: #f2f2f2;">
                <th>Excel Row #</th>
                <th>Customer ID</th>
                <th>Error Details</th>
            </tr>
            {error_rows_html}
        </table>
        """

        send_email(
            to=['b4677396@gmail.com'],
            subject=f"Quarantine Alert: {total_bad_rows} Rows Removed",
            html_content=email_body,
            files=[file_path]
        )

        #--- c. Clean bad data from silver table ---#
        delete_sql = """
            DELETE FROM silver.churn_raw
            WHERE customer_id IS NULL 
               OR tenure_in_months < 0 
               OR monthly_charges_amount < 0 
               OR gender NOT IN ('Male', 'Female')
               OR customer_id IN (
                   SELECT customer_id FROM silver.churn_raw 
                   GROUP BY customer_id HAVING COUNT(*) > 1
               );
        """
        hook.run(delete_sql)
        print("Silver table cleaned. Bad data removed.")

# #============================ this function clean the file name =======================
def clean_filename(filename):
    base_name = os.path.splitext(filename)[0]
    prefixes_to_remove = ['quarantine', 'remaining_errors', 'clean', 'fixed', 'errors']
    
    for prefix in prefixes_to_remove:
        base_name = re.sub(f"^{prefix}_", "", base_name, flags=re.IGNORECASE)
        base_name = re.sub(f"_{prefix}", "", base_name, flags=re.IGNORECASE)
        base_name = re.sub(r'\d{8}_\d{6}_', '', base_name) # يحذف التاريخ القديم

    base_name = base_name.strip('_')
    if not base_name: base_name = "data"
    return base_name

# ===================== this function loads CSV to staging table AND archives the file ==================#
def load_csv_to_staging(**kwargs):
    # 1. البحث عن الملفات الجديدة
    files = glob.glob(os.path.join(STAGING_PATH, "*.csv"))
    
    if not files:
        print(" No new files found in staging.")
        return

    hook = PostgresHook(postgres_conn_id=kwargs['conn_id'])
    engine = hook.get_sqlalchemy_engine()

    for file_path in files:
        file_name = os.path.basename(file_path) # <--- المتغير الصحيح هنا
        print(f" Processing file: {file_name}...")

        #----Read CSV into DataFrame ----#
        df = pd.read_csv(file_path)
        
        #----make column names consistent ----#
        df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
        column_mapping = {
            'customerid': 'customer_id',
            'tenure_months': 'tenure_in_months',
            'monthly_charges': 'monthly_charges_amount'
        }
        df.rename(columns=column_mapping, inplace=True)
        
        #---- Data Quality Checks ----#
        df['error_details'] = ""
        
        if 'customer_id' in df.columns:
            df.loc[df['customer_id'].isnull(), 'error_details'] += "Missing ID; "
            df.loc[df.duplicated(subset=['customer_id'], keep=False), 'error_details'] += "Duplicate ID; "
        
        if 'tenure_in_months' in df.columns:
            df.loc[df['tenure_in_months'] < 0, 'error_details'] += "Negative Tenure; "
        
        if 'monthly_charges_amount' in df.columns:
            df.loc[df['monthly_charges_amount'] < 0, 'error_details'] += "Negative Charges; "
        
        if 'gender' in df.columns:
            df.loc[~df['gender'].isin(['Male', 'Female']), 'error_details'] += "Invalid Gender; "
        
        #--- Clean up error details formatting ---#
        bad_rows = df[df['error_details'] != ""]
        good_rows = df[df['error_details'] == ""].drop(columns=['error_details'])

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # ---------------------------------------------------------
        #--------Send bad rows to Quarantine and notify -----------
        # ---------------------------------------------------------
        if not bad_rows.empty:
            print(f"⚠️ Found {len(bad_rows)} bad rows. Sending to Quarantine...")
            
            clean_base = clean_filename(file_name) 
            quarantine_name = f"quarantine_{timestamp}_{clean_base}.xlsx"
            quarantine_file_path = os.path.join(QUARANTINE_PATH, quarantine_name)
            bad_rows.to_excel(quarantine_file_path, index=False)
            
            #----send email with summary and attachment ----#
            html_table = bad_rows[['customer_id', 'error_details']].head(20).to_html(index=False, border=1, classes='table')

            send_email(
                to=['b4677396@gmail.com'],
                subject=f"⚠️ Data Rejected from {file_name}",
                html_content=f"""
                <h3>Data Quality Alert</h3>
                <p>Found <b>{len(bad_rows)}</b> bad rows. Check attachment.</p>
                {html_table}
                """,
                files=[quarantine_file_path]
            )

        # ---------------------------------------------------------
        #-----------Load good rows to DB and archive --------------
        # ---------------------------------------------------------
        if not good_rows.empty:
            print(f" Loading {len(good_rows)} clean rows to DB...")
            good_rows.to_sql('staging_churn', con=engine, if_exists='replace', index=False, schema='public')
            
            archive_name = f"clean_{timestamp}_{file_name}"
            archive_path = os.path.join(ARCHIVE_PATH, archive_name)
            good_rows.to_csv(archive_path, index=False)
            print(f" Clean data archived to: {archive_path}")

        #-- Remove original file from staging ---#
        os.remove(file_path)
        print(" Original staging file removed.")

#====================================================================================================================#
#===========================================             Start Ouer DAG          ====================================#
#====================================================================================================================#
with DAG(
    dag_id="Data_Warehouse_Full_Pipeline",
    description="Full Churn Pipeline (Bronze -> Silver -> Gold)",
    start_date=datetime(2025, 12, 9),
    schedule='6 10 * * *',
    catchup=False,
    template_searchpath=[f"{AIRFLOW_HOME}/dags/SQL"],
    tags=['churn_project', 'full_pipeline'],
    default_args=default_args
    ) as dag:
    
    conn_id = "churn_db_conn"    

    #this task debugs smtp connection
    debug_task = PythonOperator(
         task_id='debug_smtp',
         python_callable=debug_smtp_connection
    )

    #this task loads csv to staging table
    load_csv_task = PythonOperator(
        task_id='load_csv_to_staging_task',
        python_callable=load_csv_to_staging,
        op_kwargs={'conn_id': conn_id}
    )

    #this task creates the necessary tables
    create_tables_Task = SQLExecuteQueryOperator(
        task_id="create_tables_ddl",
        conn_id=conn_id,
        sql=[
            'Bronze/DDL_BronzeTable.sql',
            'Silver/DDL_Silver_Table.sql', 
            'Gold/DDL_gold.sql'
        ]
    )

    #this task fills the bronze table
    fill_bronze = SQLExecuteQueryOperator(
        task_id="insert_into_bronze",
        conn_id=conn_id,
        sql='Bronze/insert_data_into_bronze.sql'
    )

    #this task checks data quality in bronze table
    dq_bronze_check = SQLValueCheckOperator(
    task_id='dq_bronze_sanity_check',
    conn_id='churn_db_conn',
    sql=""" SELECT COUNT(*) 
            FROM bronze.churn_raw
            WHERE customer_id IS NULL 
            OR churn_label IS NULL;
         """,
    pass_value=0, 
    tolerance=0
    )
    #this task fills the silver table
    fill_silver = SQLExecuteQueryOperator(
        task_id="insert_into_silver",
        conn_id=conn_id,
        #sql='CALL silver_load_data();'
        sql='Silver/insert_data_into_silver.sql'

    )

    #this task checks data quality in silver table
    clean_silver_task = PythonOperator(
        task_id='clean_and_notify_silver_data',
        python_callable=clean_or_stop_silver,
    )
    #this task fills the gold tables
    fill_gold = SQLExecuteQueryOperator(
        task_id="insert_into_gold",
        conn_id=conn_id,
        sql='Gold/create_load_data_gold.sql'
    )

    #this task checks data quality in gold fact table
    dq_gold_check = SQLValueCheckOperator(
    task_id='dq_gold_fact_check',
    conn_id='churn_db_conn',
    sql="""
    SELECT COUNT(*) 
    FROM gold.fact_customer_churn
    WHERE 
        -- 1. التأكد إن مفاتيح الربط مش بـ Null (Orphan Records Check)
        customer_key IS NULL 
        OR contract_key IS NULL
        OR service_key IS NULL
        
        -- 2. فحص الأرقام النهائية
        OR monthly_charges < 0
        OR total_charges < 0
        
        -- 3. فحص الـ Scores
        OR churn_score < 0 
        OR churn_score > 100; -- افتراضاً إن السكور من 0 لـ 100
    """,
    pass_value=0,
    tolerance=0
)


    # ترتيب التشغيل
debug_task >> create_tables_Task >> load_csv_task >> fill_bronze >> dq_bronze_check >> fill_silver >> clean_silver_task >> fill_gold >> dq_gold_check



