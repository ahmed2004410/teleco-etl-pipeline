from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import os

# ================================================================
# Failure Notification
# ================================================================
def notify_on_failure(context):
    ti = context['task_instance']
    exception = context.get('exception', 'Unknown Error')
    try:
        send_email(
            to=['b4677396@gmail.com'],
            subject=f"❌ FAILED: {ti.task_id} — {ti.dag_id}",
            html_content=f"""
            <h3>Task Failed</h3>
            <p><b>DAG:</b> {ti.dag_id}</p>
            <p><b>Task:</b> {ti.task_id}</p>
            <p><b>Error:</b> <span style="color:red">{exception}</span></p>
            <p><b>Time:</b> {datetime.now()}</p>
            <p><a href="{ti.log_url}">View Logs</a></p>
            """
        )
    except Exception as e:
        print(f"⚠️ Could not send failure email: {e}")


default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_on_failure,
}

SQL_QUERY = """
SELECT 
    u.id AS customer_id,
    COALESCE(u.gender, 'Male') AS gender,
    'No' AS senior_citizen,
    COALESCE(p.partner, 'No') AS partner,
    CASE WHEN p.dependents THEN 'Yes' ELSE 'No' END AS dependents,
    'Egypt' AS country,
    COALESCE(u.region, 'Cairo') AS state,
    COALESCE(u.region, 'Cairo') AS city,
    30753 AS zip_code,
    30.0444 AS latitude, 
    31.2357 AS longitude,
    CASE WHEN p."phoneService" THEN 'Yes' ELSE 'No' END AS phone_service,
    CASE WHEN p."multipleLines" THEN 'Yes' ELSE 'No' END AS multiple_lines,
    COALESCE(p."internetService", 'Fiber optic') AS internet_service,
    CASE WHEN p."onlineSecurity" THEN 'Yes' ELSE 'No' END AS online_security,
    CASE WHEN p."onlineBackup" THEN 'Yes' ELSE 'No' END AS online_backup,
    CASE WHEN p."deviceProtection" THEN 'Yes' ELSE 'No' END AS device_protection,
    CASE WHEN p."techSupport" THEN 'Yes' ELSE 'No' END AS tech_support,
    CASE WHEN p."streamingTV" THEN 'Yes' ELSE 'No' END AS streaming_tv,
    CASE WHEN p."streamingMovies" THEN 'Yes' ELSE 'No' END AS streaming_movies,
    CASE WHEN p."paperlessBilling" THEN 'Yes' ELSE 'No' END AS paperless_billing,
    COALESCE(p."paymentMethod", 'Electronic check') AS payment_method,
    COALESCE(p."contractType", 'Month-to-month') AS contract,
    (EXTRACT(YEAR FROM age(NOW(), u."createdAt")) * 12 + EXTRACT(MONTH FROM age(NOW(), u."createdAt"))) AS tenure_in_months,
    p."monthlyCharges" AS monthly_charges_amount,
    COALESCE((SELECT SUM(amount) FROM "BillingHistory" WHERE "userId" = u.id), 0) AS total_charges,
    CASE WHEN u.status = 'blocked' THEN 'Yes' ELSE 'No' END AS churn_label,
    CASE WHEN u.status = 'blocked' THEN '1' ELSE '0' END AS churn_value,
    'n/a' AS churn_score,
    'n/a' AS cltv,
    'n/a' AS churn_reason
FROM "User" u
LEFT JOIN "UserPersonalization" p ON u.id = p."userId"
"""

# ================================================================
# Main Function
# ================================================================
def export_data_to_csv(**context):

    # ✅ Docker-compatible path
    # جوا الـ Container: /usr/local/airflow/include/staging
    # على جهازك:        D:\teleco-etl-pipeline\include\staging  (عن طريق volume mount)
    airflow_home = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')
    export_dir = os.path.join(airflow_home, 'include', 'staging')
    os.makedirs(export_dir, exist_ok=True)

    # Query the database
    try:
        hook = PostgresHook(postgres_conn_id='backend_postgres_db')
        df = hook.get_pandas_df(SQL_QUERY)
    except Exception as e:
        raise Exception(f"❌ Database extraction failed: {e}")

    # Check if data is empty
    if df.empty:
        print("⚠️ Query returned 0 rows. No CSV will be written.")
        raise AirflowSkipException("No data found — skipping export to avoid empty CSV in staging.")

    # Log row count
    print(f"📊 Extracted {len(df):,} rows × {len(df.columns)} columns from source DB.")

    # Write CSV
    file_path = os.path.join(
        export_dir,
        f"Churn_Export_{datetime.now().strftime('%Y%m%d')}.csv"
    )

    try:
        df.to_csv(file_path, index=False)
        print(f"✅ Data exported successfully → {file_path}")
    except Exception as e:
        raise Exception(f"❌ Failed to write CSV to disk: {e}")

    # Push file path to XCom for the next DAG to pick up
    context['ti'].xcom_push(key='exported_file_path', value=file_path)


# ================================================================
# DAG Definition
# ================================================================
with DAG(
    'daily_churn_export_pipeline_cloude',
    default_args=default_args,
    description='Extract Churn data from Postgres to CSV',
    schedule='@daily',
    catchup=False,
    tags=['churn_project', 'extraction'],
) as dag:

    export_task = PythonOperator(
        task_id='export_postgres_to_csv',
        python_callable=export_data_to_csv,
    )

    export_task