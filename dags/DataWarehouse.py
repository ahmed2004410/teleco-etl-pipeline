from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator, SQLValueCheckOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
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
# ===============================================================
#------ This section contains failure notification functions ----
# ===============================================================
def send_slack_alert(context):
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_date = context.get('execution_date')
    log_url = ti.log_url
    exception = context.get('exception')
    error_msg = str(exception) if exception else "Unknown Error"

    slack_msg = f"""
    :red_circle: *Task Failed!*
    *DAG*: `{dag_id}`
    *Task*: `{task_id}`
    *Time*: `{execution_date}`
    *Error*: ```{error_msg}```
    < {log_url} | View Logs >
    """

    try:
        slack_hook = SlackWebhookHook(slack_webhook_conn_id='slack_conn')
        slack_hook.send(text=slack_msg)
        print(" Slack notification sent successfully!")
    except Exception as e:
        print(f" Failed to send Slack notification: {e}")

# ===============================================================
# ------------This function sends email on failure --------------
# ===============================================================
def notify_email_on_failure(context):
    task_instance = context['task_instance']
    exception = context.get('exception') 
    error_message = str(exception) if exception else "Unknown Error"
    
    try: 
        print(" Attempting to send failure email manually...")
        send_email(
            to=['b4677396@gmail.com'],
            subject=f" FAILED: {task_instance.task_id}",
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

# ===============================================================
# ---------- This is the master failure callback function -------
# ===============================================================
def failure_callback_manager(context):
    send_slack_alert(context)
    notify_email_on_failure(context)

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'on_failure_callback': failure_callback_manager, 
    'email_on_failure': False, 
}

# ===============================================================
# ----------- This function debugs SMTP connection --------------
# ===============================================================
def debug_smtp_connection():
    try:
        conn = BaseHook.get_connection("smtp_default")
        print(f" Detective Airflow Report:")
        print(f"   -> Host detected: {conn.host}")
        print(f"   -> Login detected: {conn.login}")
        print(f"   -> Port detected: {conn.port}")
    except Exception as e:
        print(f" Could not find smtp_default connection: {e}")

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'on_failure_callback': failure_callback_manager, 
    'email_on_failure': False, 
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
    total_rows_sql = "SELECT COUNT(*) FROM silver.churn_raw"
    total_rows = hook.get_first(total_rows_sql)[0]
    
    total_bad_rows = len(df_bad)
    if total_rows == 0: 
        error_rate = 0 
    else:
        error_rate = (total_bad_rows / total_rows) * 100
    # الحد المسموح به للخطأ (10%)
    ERROR_RATE_THRESHOLD = 10.0 

    # إذا تجاوزت النسبة الحد المسموح
    if error_rate > ERROR_RATE_THRESHOLD:
        print(f" CRITICAL: Error rate ({error_rate:.2f}%) exceeded threshold ({ERROR_RATE_THRESHOLD}%)!")
        
        send_email(
            to=['b4677396@gmail.com'],
            subject=f" PIPELINE STOPPED: High Error Rate ({error_rate:.2f}%)",
            html_content=f"""
            <h3>Critical Data Quality Issue</h3>
            <p>The pipeline stopped because <b>{total_bad_rows}</b> bad rows were found.</p>
            <p>Error Rate: <b>{error_rate:.2f}%</b> (Threshold: {ERROR_RATE_THRESHOLD}%).</p>
            <p>Please check the database table <i>silver.churn_raw</i> immediately.</p>
            """
        )
        # إيقاف البايب لاين برسالة واضحة
        raise AirflowException(f"Pipeline stopped. Error rate too high ({error_rate:.2f}%).")

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

#============================ this function clean the file name =======================
def clean_filename(filename):
    base_name = os.path.splitext(filename)[0]
    prefixes_to_remove = ['quarantine', 'remaining_errors', 'clean', 'fixed', 'errors']
    
    for prefix in prefixes_to_remove:
        base_name = re.sub(f"^{prefix}_", "", base_name, flags=re.IGNORECASE)
        base_name = re.sub(f"_{prefix}", "", base_name, flags=re.IGNORECASE)
        base_name = re.sub(r'\d{8}_\d{6}_', '', base_name) 

    base_name = base_name.strip('_')
    if not base_name: base_name = "data"
    return base_name

# ===================== this function loads CSV to staging table AND archives the file ==================#
def load_csv_to_staging(**kwargs):
    files = glob.glob(os.path.join(STAGING_PATH, "*.csv"))
    
    if not files:
        print("ℹ️ No new files found in staging.")
        return [] 
        
    hook = PostgresHook(postgres_conn_id=kwargs['conn_id'])
    engine = hook.get_sqlalchemy_engine()
    
    hook.run("TRUNCATE TABLE staging_churn")

    processed_files = [] 
    
    for file_path in files:
        file_name = os.path.basename(file_path).strip()
        print(f"📂 Loading file to Staging: {file_name}...")

        # قراءة الملف بـ Pandas
        df = pd.read_csv(file_path)
        
        df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
        column_mapping = {
            'customerid': 'customer_id',
            'tenure_months': 'tenure_in_months',
            'monthly_charges': 'monthly_charges_amount'
        }
        df.rename(columns=column_mapping, inplace=True)
        df['error_details'] = ""

        good_rows = df[df['error_details'] == ""]
        
        if not good_rows.empty:
            good_rows_to_load = good_rows.drop(columns=['error_details'])
            
            # 2. املأ الـ Staging بالداتا الجديدة
            good_rows_to_load.to_sql('staging_churn', con=engine, if_exists='append', index=False, schema='public')
            print(f"📥 Loaded {len(good_rows_to_load)} rows into Staging.")


            print("🔍 Validating Staging data against Bronze History...")
            
            # الكويري ده بيشوف هل فيه أي Customer ID في الـ Staging موجود قبل كدة في الـ Bronze؟
            check_duplication_sql = """
                SELECT COUNT(*) 
                FROM staging_churn s
                JOIN bronze.churn_raw b ON s.customer_id = b.customer_id;
            """
            
            duplicates_count = hook.get_first(check_duplication_sql)[0]

            if duplicates_count > 0:
                msg = f"⛔ STOP! Found {duplicates_count} customers already exist in Bronze!"
                print(msg)
                
                # أ. ابعت إيميل بالخطأ
                send_email(
                    to=['b4677396@gmail.com'],
                    subject=f"🚨 DUPLICATE DATA BLOCKED: {file_name}",
                    html_content=f"""
                    <h3>Pipeline Stopped by Validator</h3>
                    <p>File <b>{file_name}</b> contains <b>{duplicates_count}</b> records that are already in the Bronze layer.</p>
                    <p><b>Action:</b> Staging truncated, File moved to Quarantine.</p>
                    """
                )
                
                # ب. فضي الـ Staging فوراً عشان مفيش حاجة غلط تعدي
                hook.run("TRUNCATE TABLE staging_churn")
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                quarantine_dest = os.path.join(QUARANTINE_PATH, f"DUP_CONTENT_{timestamp}_{file_name}")
                shutil.move(file_path, quarantine_dest)
                
                raise AirflowException(msg)

            else:
                print("✅ Validation Passed: No duplicates found in Bronze.")
        
        # ... (تسجيل الـ Logs في audit table...)
        processed_files.append(file_path)

    return processed_files

# ===============================================================
# --------- This function archives processed files --------------
# ===============================================================
def archive_processed_files(**context):
    ti = context['ti']
    file_paths = ti.xcom_pull(task_ids='load_csv_to_staging_task')
    
    if not file_paths:
        print("No files to archive.")
        return

    # 2. نقل الملفات للأرشيف
    for file_path in file_paths:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            
            # إضافة Timestamp للاسم عشان لو الملف اتكرر
            timestamp = datetime.now().strftime("%Y%m%d")
            archive_name = f"{file_name}_date_{timestamp}.csv"
            archive_full_path = os.path.join(ARCHIVE_PATH, archive_name)
            
            # نقل الملف (Move)
            shutil.move(file_path, archive_full_path)
            print(f"✅ File successfully moved to archive: {archive_full_path}")
        else:
            print(f"⚠️ File not found (maybe moved already?): {file_path}")


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
         python_callable= debug_smtp_connection
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

    archive_task = PythonOperator(
        task_id='archive_files_task',
        python_callable=archive_processed_files,
        trigger_rule='all_success' 
    )

    #  ترتيب التشغيل 
    debug_task >> create_tables_Task >> load_csv_task >> fill_bronze >> dq_bronze_check >> fill_silver >> clean_silver_task >> fill_gold >> dq_gold_check >> archive_task



