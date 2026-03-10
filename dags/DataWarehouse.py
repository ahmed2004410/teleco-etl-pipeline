from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator, SQLValueCheckOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from scripts.train_churn_model import train_and_predict_churn
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.utils.email import send_email 
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime
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
# -------- Metadata Table DDL (Pipeline State Management) -------
# ===============================================================
METADATA_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS public.pipeline_file_metadata (
    id                  SERIAL PRIMARY KEY,
    file_name           VARCHAR(500)    NOT NULL UNIQUE,
    file_path           TEXT            NOT NULL,
    file_size_bytes     BIGINT,
    row_count           INTEGER,
    status              VARCHAR(50)     NOT NULL DEFAULT 'PENDING',
    error_message       TEXT,
    processed_at        TIMESTAMP,
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    dag_run_id          VARCHAR(250),
    checksum_md5        VARCHAR(64)
);
"""

# ===============================================================
# -------- Bootstrap: ensure metadata table exists on import ----
# ===============================================================
def _bootstrap_metadata_table():
    """Called once at DAG parse time to guarantee the table exists."""
    try:
        hook = PostgresHook(postgres_conn_id='churn_db_conn')
        hook.run(METADATA_TABLE_DDL)
        print("✅ pipeline_file_metadata table is ready.")
    except Exception as e:
        print(f"⚠️  Could not bootstrap metadata table: {e}")

# ===============================================================
# ----- Incremental Load Helpers --------------------------------
# ===============================================================
import hashlib

def _md5_of_file(path: str) -> str:
    """Return MD5 hex-digest of a file (for change detection)."""
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()

def _get_new_files(hook: PostgresHook, file_list: list) -> list:
    """
    Compare file_list against the metadata table and return only
    files that are truly NEW (never seen before, or previously FAILED).

    A file is skipped if it has status = 'SUCCESS' AND its checksum
    hasn't changed since the last successful run.
    """
    if not file_list:
        return []

    new_files = []
    for fp in file_list:
        fname   = os.path.basename(fp)
        chk     = _md5_of_file(fp)

        row = hook.get_first(
            """
            SELECT status, checksum_md5
            FROM   public.pipeline_file_metadata
            WHERE  file_name = %s
            ORDER  BY created_at DESC
            LIMIT  1
            """,
            parameters=(fname,)
        )

        if row is None:
            # Never seen → process it
            new_files.append(fp)
            print(f"🆕  NEW file detected: {fname}")

        elif row[0] == 'SUCCESS' and row[1] == chk:
            # Already processed, file unchanged → SKIP
            print(f"⏭️  SKIPPING (already processed, unchanged): {fname}")

        else:
            # Previously FAILED or file content changed → retry
            new_files.append(fp)
            print(f"🔄  RE-QUEUED (status={row[0]}, checksum changed={row[1] != chk}): {fname}")

    return new_files

def _register_file(hook: PostgresHook, fp: str, status: str,
                   row_count: int = None, error_msg: str = None,
                   dag_run_id: str = None):
    """
    Upsert a record in pipeline_file_metadata.
    Uses INSERT … ON CONFLICT to handle both first-time and retry runs.
    """
    fname = os.path.basename(fp)
    fsize = os.path.getsize(fp) if os.path.exists(fp) else None
    chk   = _md5_of_file(fp)    if os.path.exists(fp) else None

    hook.run(
        """
        INSERT INTO public.pipeline_file_metadata
            (file_name, file_path, file_size_bytes, row_count,
             status, error_message, processed_at, dag_run_id, checksum_md5)
        VALUES
            (%s, %s, %s, %s, %s, %s, NOW(), %s, %s)
        ON CONFLICT (file_name) DO UPDATE SET
            status        = EXCLUDED.status,
            error_message = EXCLUDED.error_message,
            processed_at  = EXCLUDED.processed_at,
            row_count     = EXCLUDED.row_count,
            file_size_bytes = EXCLUDED.file_size_bytes,
            dag_run_id    = EXCLUDED.dag_run_id,
            checksum_md5  = EXCLUDED.checksum_md5;
        """,
        parameters=(fname, fp, fsize, row_count,
                    status, error_msg, dag_run_id, chk)
    )
    print(f"📋 Metadata updated → {fname} : {status}")

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
# ------------This function sends email on success --------------
# ===============================================================
DEFAULT_RECIPIENTS = ["b4677396@gmail.com"]
AIRFLOW_BASE_URL = Variable.get("airflow_base_url", default_var="https://your-airflow-instance")


def notify_pipeline_success(context):
    """
    Airflow success callback — sends a structured HTML summary email
    when a full DAG run completes successfully.

    Reads recipients from the Airflow Variable `pipeline_alert_recipients`
    (comma-separated). Falls back to DEFAULT_RECIPIENTS if the variable
    is not set.
    """
    try:
        # ── Extract context variables ────────────────────────────────────────
        dag_id    = context["dag"].dag_id
        run_id    = context["run_id"]
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")

        # ── Deep-link to the Airflow Grid view for this specific run ─────────
        run_url = (
            f"{AIRFLOW_BASE_URL}/dags/{dag_id}/grid"
            f"?dag_run_id={run_id}"
        )

        # ── Config-driven recipient list (no hardcoding) ─────────────────────
        raw_recipients = Variable.get(
            "pipeline_alert_recipients",
            default_var=",".join(DEFAULT_RECIPIENTS),
        )
        recipients = [r.strip() for r in raw_recipients.split(",") if r.strip()]

        # ── Email content ────────────────────────────────────────────────────
        subject = f"✅ {dag_id} — Pipeline Run Complete"

        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
          <meta charset="UTF-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
          <title>{subject}</title>
          <style>
            body      {{ font-family: Arial, sans-serif; background: #f4f4f4;
                         margin: 0; padding: 0; color: #333; }}
            .wrapper  {{ max-width: 600px; margin: 32px auto; background: #fff;
                         border-radius: 8px; overflow: hidden;
                         box-shadow: 0 2px 8px rgba(0,0,0,.08); }}
            .header   {{ background: #1a1a2e; padding: 28px 32px; }}
            .header h1{{ color: #fff; margin: 0; font-size: 20px;
                         font-weight: 600; letter-spacing: .3px; }}
            .header p {{ color: #a0a8c0; margin: 6px 0 0; font-size: 13px; }}
            .body     {{ padding: 28px 32px; }}
            .body p   {{ line-height: 1.6; font-size: 14px; }}
            table     {{ width: 100%; border-collapse: collapse;
                         margin: 20px 0; font-size: 13px; }}
            th        {{ background: #f0f2f5; text-align: left;
                         padding: 10px 12px; color: #555; font-weight: 600;
                         border-bottom: 2px solid #e0e0e0; }}
            td        {{ padding: 10px 12px; border-bottom: 1px solid #eee;
                         vertical-align: middle; }}
            tr:last-child td {{ border-bottom: none; }}
            .badge    {{ display: inline-block; background: #e6f9f0;
                         color: #1a8a5a; border-radius: 4px; padding: 2px 8px;
                         font-size: 12px; font-weight: 600; }}
            .cta      {{ display: block; width: fit-content; margin: 24px 0;
                         background: #1a1a2e; color: #fff !important;
                         text-decoration: none; padding: 12px 24px;
                         border-radius: 6px; font-size: 14px;
                         font-weight: 600; letter-spacing: .3px; }}
            .footer   {{ background: #f9f9f9; padding: 16px 32px;
                         font-size: 12px; color: #999;
                         border-top: 1px solid #eee; }}
            .footer a {{ color: #666; text-decoration: none; }}
          </style>
        </head>
        <body>
          <div class="wrapper">

            <!-- Header -->
            <div class="header">
              <h1>✅ Pipeline Completed Successfully</h1>
              <p>Your data is fresh and ready for downstream consumption.</p>
            </div>

            <!-- Body -->
            <div class="body">
              <p>
                The <strong>{dag_id}</strong> pipeline has finished its full run
                without errors. All layers of your data architecture have been
                refreshed and are available immediately.
              </p>

              <!-- Run Summary -->
              <table>
                <tr>
                  <th colspan="2">📋 Run Summary</th>
                </tr>
                <tr>
                  <td><strong>Pipeline</strong></td>
                  <td>{dag_id}</td>
                </tr>
                <tr>
                  <td><strong>Run ID</strong></td>
                  <td><code>{run_id}</code></td>
                </tr>
                <tr>
                  <td><strong>Completed At</strong></td>
                  <td>{timestamp}</td>
                </tr>
                <tr>
                  <td><strong>Status</strong></td>
                  <td><span class="badge">✅ Success</span></td>
                </tr>
              </table>

              <!-- Layer Status -->
              <table>
                <tr>
                  <th>Layer</th>
                  <th>Purpose</th>
                  <th>Status</th>
                </tr>
                <tr>
                  <td>🥉 <strong>Bronze</strong></td>
                  <td>Raw ingestion</td>
                  <td><span class="badge">✅ Updated</span></td>
                </tr>
                <tr>
                  <td>🥈 <strong>Silver</strong></td>
                  <td>Cleaned &amp; transformed</td>
                  <td><span class="badge">✅ Updated</span></td>
                </tr>
                <tr>
                  <td>🥇 <strong>Gold</strong></td>
                  <td>Business-ready aggregates</td>
                  <td><span class="badge">✅ Updated</span></td>
                </tr>
              </table>

              <!-- CTA -->
              <a class="cta" href="{run_url}">→ View Run in Airflow</a>

              <p style="font-size:13px; color:#777;">
                No action is required. If this run was unexpected or you have
                questions, contact your data team or visit the
                <a href="{AIRFLOW_BASE_URL}/docs">Help Center</a>.
              </p>
            </div>

            <!-- Footer -->
            <div class="footer">
              You are receiving this because you are subscribed to pipeline alerts.
              &nbsp;·&nbsp;
              <a href="{AIRFLOW_BASE_URL}">Airflow Dashboard</a>
              &nbsp;·&nbsp;
              <a href="{AIRFLOW_BASE_URL}/docs">Help Center</a>
            </div>

          </div>
        </body>
        </html>
        """

        # ── Send ─────────────────────────────────────────────────────────────
        send_email(
            to=recipients,
            subject=subject,
            html_content=html_content,
        )

        print(f"--- Pipeline success email sent → {recipients} ---")

    except Exception as e:
        print(f"[notify_pipeline_success] Failed to send email: {e}")
        raise   # re-raise so Airflow marks the callback as failed, not silently swallowed


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
    #'on_success_callback': notify_success, # السطر ده هو اللي هيشغل إيميل النجاح
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
    """
    Incremental / Idempotent loader.
    
    Flow:
      1. Discover all CSV files in STAGING_PATH.
      2. Ask the metadata table which ones are truly NEW (delta).
      3. For each new file:
           a. Register it as PROCESSING to prevent parallel re-runs.
           b. Validate row-level quality; quarantine bad rows.
           c. Check for duplicates vs Bronze layer.
           d. Load clean rows into staging_churn.
           e. Mark the file SUCCESS (or FAILED on error).
      4. Return only the successfully processed file paths for archiving.
    """
    dag_run_id = kwargs.get('run_id', 'unknown')
    conn_id    = kwargs['conn_id']

    # ── 1. Discover files ──────────────────────────────────────────
    all_files = glob.glob(os.path.join(STAGING_PATH, "*.csv"))
    if not all_files:
        print("ℹ️  No CSV files found in staging folder.")
        return []

    hook   = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    # ── 2. Filter: keep only NEW / FAILED files (the delta) ───────
    new_files = _get_new_files(hook, all_files)

    if not new_files:
        print("✅  All files already processed. Nothing to do — pipeline is up-to-date.")
        return []

    print(f"📦  {len(new_files)} new file(s) to process out of {len(all_files)} total.")

    # Truncate staging once before the batch so we start clean
    hook.run("TRUNCATE TABLE staging_churn")

    processed_files = []

    for file_path in new_files:
        file_name = os.path.basename(file_path).strip()
        print(f"\n📂  Processing: {file_name}")

        # ── a. Mark as IN-PROGRESS ─────────────────────────────────
        _register_file(hook, file_path, status='PROCESSING',
                       dag_run_id=dag_run_id)

        try:
            # ── b. Read & normalise columns ────────────────────────
            df = pd.read_csv(file_path)
            df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
            df.rename(columns={
                'customerid':      'customer_id',
                'tenure_months':   'tenure_in_months',
                'monthly_charges': 'monthly_charges_amount'
            }, inplace=True)

            total_rows = len(df)

            # ── b2. Row-level validation (build error_details) ─────
            df['error_details'] = ""
            df.loc[df['customer_id'].isnull(),                          'error_details'] += "Missing ID; "
            df.loc[df.get('tenure_in_months',  pd.Series(dtype=float)) < 0, 'error_details'] += "Negative Tenure; "
            df.loc[df.get('monthly_charges_amount', pd.Series(dtype=float)) < 0, 'error_details'] += "Negative Charges; "
            if 'gender' in df.columns:
                df.loc[~df['gender'].isin(['Male', 'Female']),          'error_details'] += "Invalid Gender; "
            dup_ids = df[df.duplicated(subset=['customer_id'], keep=False)]['customer_id'].dropna().unique()
            df.loc[df['customer_id'].isin(dup_ids),                     'error_details'] += "Duplicate ID; "
            df['error_details'] = df['error_details'].str.strip('; ')

            bad_rows  = df[df['error_details'] != ""]
            good_rows = df[df['error_details'] == ""].drop(columns=['error_details'])

            # Export bad rows to quarantine (non-blocking)
            if not bad_rows.empty:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                q_path = os.path.join(QUARANTINE_PATH, f"quarantine_{ts}_{file_name}.xlsx")
                bad_rows.to_excel(q_path, index=False)
                print(f"⚠️  {len(bad_rows)} bad row(s) quarantined → {q_path}")

            if good_rows.empty:
                raise AirflowException(f"File '{file_name}' has 0 valid rows after quality checks.")

            # ── c. Duplicate check against Bronze history ──────────
            good_rows.to_sql('staging_churn', con=engine,
                             if_exists='append', index=False, schema='public')

            dup_count = hook.get_first("""
                SELECT COUNT(*)
                FROM   staging_churn s
                JOIN   bronze.churn_raw b ON s.customer_id = b.customer_id
            """)[0]

            if dup_count > 0:
                hook.run("TRUNCATE TABLE staging_churn")
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                shutil.move(file_path,
                            os.path.join(QUARANTINE_PATH, f"DUP_CONTENT_{ts}_{file_name}"))

                err_msg = f"Blocked: {dup_count} customer(s) already exist in Bronze."
                _register_file(hook, file_path, status='FAILED',
                               error_msg=err_msg, dag_run_id=dag_run_id)

                send_email(
                    to=['b4677396@gmail.com'],
                    subject=f"🚨 DUPLICATE DATA BLOCKED: {file_name}",
                    html_content=f"""
                    <h3>Pipeline Stopped by Validator</h3>
                    <p>File <b>{file_name}</b> contains <b>{dup_count}</b> records already in Bronze.</p>
                    <p><b>Action:</b> Staging truncated, file moved to Quarantine.</p>
                    """
                )
                raise AirflowException(err_msg)

            print(f"✅  Validation passed. {len(good_rows)} clean row(s) staged.")

            # ── d. Mark SUCCESS ────────────────────────────────────
            _register_file(hook, file_path, status='SUCCESS',
                           row_count=len(good_rows), dag_run_id=dag_run_id)

            processed_files.append(file_path)

        except AirflowException:
            raise   # let Airflow handle it, already registered as FAILED above

        except Exception as exc:
            err_msg = str(exc)
            _register_file(hook, file_path, status='FAILED',
                           error_msg=err_msg, dag_run_id=dag_run_id)
            print(f"❌  Unexpected error on {file_name}: {err_msg}")
            raise AirflowException(f"Failed processing {file_name}: {err_msg}")

    print(f"\n🏁  Incremental load complete. {len(processed_files)}/{len(new_files)} file(s) loaded successfully.")
    return processed_files

# ===============================================================
# --------- This function archives processed files --------------
# ===============================================================
def archive_processed_files(**context):
    hook = PostgresHook(postgres_conn_id='churn_db_conn')
    
    # جيب الملفات اللي status بتاعها SUCCESS ولسه في staging
    rows = hook.get_records("""
        SELECT file_path FROM public.pipeline_file_metadata
        WHERE status = 'SUCCESS'
        AND processed_at::DATE = CURRENT_DATE
    """)

    if not rows:
        print("No files to archive.")
        return

    for (file_path,) in rows:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            timestamp = datetime.now().strftime("%Y%m%d")
            archive_name = f"{file_name}_date_{timestamp}.csv"
            archive_full_path = os.path.join(ARCHIVE_PATH, archive_name)
            shutil.move(file_path, archive_full_path)
            print(f"✅ Archived: {archive_full_path}")
        else:
            print(f"⚠️ Already moved: {file_path}")

#====================================================================================================================#
#===========================================             Start Ouer DAG          ====================================#
#====================================================================================================================#

# Ensure the metadata table exists every time this DAG file is parsed by the scheduler
_bootstrap_metadata_table()

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

    #this task loads csv to staging table (incremental / idempotent)
    load_csv_task = PythonOperator(
        task_id='load_csv_to_staging_task',
        python_callable=load_csv_to_staging,
        op_kwargs={
            'conn_id': conn_id,
            'run_id' : '{{ run_id }}'   # Jinja → unique per DAG run for audit trail
        }
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
        -- 1. Orphan Records Check
        customer_key IS NULL 
        OR contract_key IS NULL
        OR service_key IS NULL
        
        -- 2. Numeric validity
        OR monthly_charges < 0
        OR total_charges < 0
        
        -- 3. Churn score (NULL allowed; 'n/a' converted to NULL in load)
        OR (churn_score IS NOT NULL AND (churn_score < 0 OR churn_score > 100))
    """,
    pass_value=0,
    tolerance=0,
    on_success_callback=notify_pipeline_success
    )

    # ML task: train XGBoost churn model and save predictions to gold.churn_predictions
    train_churn_task = PythonOperator(
        task_id='train_churn_model_task',
        python_callable=train_and_predict_churn,
        op_kwargs={'conn_id': conn_id},
    )

    archive_task = PythonOperator(
    task_id='archive_files_task',
    python_callable=archive_processed_files,
    trigger_rule=TriggerRule.ALL_DONE  # بدل all_success
)

    #  ترتيب التشغيل 
    debug_task >> create_tables_Task >> load_csv_task >> fill_bronze >> dq_bronze_check >> fill_silver >> clean_silver_task >> fill_gold >> dq_gold_check >> train_churn_task >> archive_task