from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from datetime import datetime, timedelta
import os

# ================================================================
# Config
# ================================================================
WATERMARK_VARIABLE_KEY = "churn_export_last_watermark"
INITIAL_LOAD_DATE      = "1970-01-01 00:00:00"


def _get_recipients():
    return [
        r.strip()
        for r in Variable.get(
            "pipeline_alert_recipients",
            default_var="b4677396@gmail.com"
        ).split(",")
        if r.strip()
    ]


def _get_airflow_url():
    return Variable.get("airflow_base_url", default_var="http://localhost:8080")


# ================================================================
# Email Styles (shared across all templates)
# ================================================================
EMAIL_STYLES = """
<style>
  body      { font-family: Arial, sans-serif; background:#f4f4f4; margin:0; padding:0; color:#333; }
  .wrapper  { max-width:620px; margin:32px auto; background:#fff; border-radius:8px;
               overflow:hidden; box-shadow:0 2px 8px rgba(0,0,0,.08); }
  .header   { padding:28px 32px; }
  .header h1{ color:#fff; margin:0; font-size:20px; font-weight:600; letter-spacing:.3px; }
  .header p { color:rgba(255,255,255,.75); margin:6px 0 0; font-size:13px; }
  .body     { padding:28px 32px; }
  .body p   { line-height:1.6; font-size:14px; }
  table     { width:100%; border-collapse:collapse; margin:20px 0; font-size:13px; }
  th        { background:#f0f2f5; text-align:left; padding:10px 12px; color:#555;
               font-weight:600; border-bottom:2px solid #e0e0e0; }
  td        { padding:10px 12px; border-bottom:1px solid #eee; vertical-align:middle; }
  tr:last-child td { border-bottom:none; }
  .badge-green  { display:inline-block; background:#e6f9f0; color:#1a8a5a;
                  border-radius:4px; padding:2px 8px; font-size:12px; font-weight:600; }
  .badge-blue   { display:inline-block; background:#e8f0fe; color:#1a56db;
                  border-radius:4px; padding:2px 8px; font-size:12px; font-weight:600; }
  .badge-orange { display:inline-block; background:#fff3e0; color:#e65100;
                  border-radius:4px; padding:2px 8px; font-size:12px; font-weight:600; }
  .badge-red    { display:inline-block; background:#fdecea; color:#c0392b;
                  border-radius:4px; padding:2px 8px; font-size:12px; font-weight:600; }
  .stat-grid { display:flex; gap:12px; margin:20px 0; }
  .stat-box  { flex:1; background:#f8f9fa; border-radius:6px; padding:14px 16px;
               text-align:center; border:1px solid #e9ecef; }
  .stat-box .num  { font-size:28px; font-weight:700; margin:0; }
  .stat-box .lbl  { font-size:11px; color:#888; margin-top:4px; text-transform:uppercase;
                    letter-spacing:.5px; }
  .cta      { display:inline-block; background:#1a1a2e; color:#fff !important;
               text-decoration:none; padding:12px 24px; border-radius:6px;
               font-size:14px; font-weight:600; letter-spacing:.3px; margin:16px 0; }
  .footer   { background:#f9f9f9; padding:16px 32px; font-size:12px; color:#999;
               border-top:1px solid #eee; }
  .footer a { color:#666; text-decoration:none; }
  code      { background:#f0f2f5; padding:2px 6px; border-radius:3px;
               font-size:12px; color:#555; }
</style>
"""


# ================================================================
# ❌ Failure Notification
# ================================================================
def notify_on_failure(context):
    ti          = context['task_instance']
    exception   = context.get('exception', 'Unknown Error')
    airflow_url = _get_airflow_url()

    try:
        send_email(
            to=_get_recipients(),
            subject=f"❌ FAILED: {ti.task_id} — {ti.dag_id}",
            html_content=f"""
            <!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">{EMAIL_STYLES}</head>
            <body><div class="wrapper">

              <div class="header" style="background:#c0392b;">
                <h1>❌ Task Failed</h1>
                <p>Immediate attention required — the export did not complete.</p>
              </div>

              <div class="body">
                <table>
                  <tr><th colspan="2">🔍 Failure Details</th></tr>
                  <tr><td><strong>DAG</strong></td><td><code>{ti.dag_id}</code></td></tr>
                  <tr><td><strong>Task</strong></td><td><code>{ti.task_id}</code></td></tr>
                  <tr><td><strong>Run ID</strong></td><td><code>{ti.run_id}</code></td></tr>
                  <tr><td><strong>Failed At</strong></td><td>{datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</td></tr>
                  <tr><td><strong>Status</strong></td><td><span class="badge-red">❌ Failed</span></td></tr>
                </table>

                <table>
                  <tr><th>⚠️ Error Message</th></tr>
                  <tr><td style="color:#c0392b; font-family:monospace; font-size:13px;">{exception}</td></tr>
                </table>

                <a class="cta" href="{ti.log_url}">→ View Full Logs</a>

                <p style="font-size:13px; color:#777;">
                  The pipeline has <strong>not</strong> written any CSV and the watermark
                  has <strong>not</strong> advanced — the next scheduled run will
                  retry the same window automatically.
                </p>
              </div>

              <div class="footer">
                Churn Export Pipeline Alert &nbsp;·&nbsp;
                <a href="{airflow_url}">Airflow Dashboard</a>
              </div>
            </div></body></html>
            """
        )
    except Exception as e:
        print(f"Could not send failure email: {e}")


# ================================================================
# ⏭️ Skip Notification  (no new data)
# ================================================================
def notify_on_skip(dag_id, run_id, watermark_from, watermark_to, log_url):
    airflow_url = _get_airflow_url()
    try:
        send_email(
            to=_get_recipients(),
            subject=f"⏭️ SKIPPED: {dag_id} — No New Data",
            html_content=f"""
            <!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">{EMAIL_STYLES}</head>
            <body><div class="wrapper">

              <div class="header" style="background:#5c6bc0;">
                <h1>⏭️ Pipeline Skipped — No New Data</h1>
                <p>The export ran successfully but found nothing new to export.</p>
              </div>

              <div class="body">
                <p>
                  The <strong>{dag_id}</strong> pipeline completed its check but found
                  <strong>zero new or updated records</strong> within the watermark window.
                  No CSV was written — this is expected behaviour on quiet days.
                </p>

                <table>
                  <tr><th colspan="2">📋 Run Summary</th></tr>
                  <tr><td><strong>DAG</strong></td><td><code>{dag_id}</code></td></tr>
                  <tr><td><strong>Run ID</strong></td><td><code>{run_id}</code></td></tr>
                  <tr><td><strong>Window From</strong></td><td>{watermark_from}</td></tr>
                  <tr><td><strong>Window To</strong></td><td>{watermark_to}</td></tr>
                  <tr><td><strong>Status</strong></td><td><span class="badge-blue">⏭️ Skipped</span></td></tr>
                  <tr><td><strong>CSV Written</strong></td><td>No</td></tr>
                  <tr><td><strong>Watermark Advanced</strong></td><td>No — stays at {watermark_from}</td></tr>
                </table>

                <p style="font-size:13px; color:#777;">
                  No action needed. The next run will check from
                  <strong>{watermark_from}</strong> onwards again.
                </p>

                <a class="cta" href="{log_url}">→ View Logs</a>
              </div>

              <div class="footer">
                Churn Export Pipeline Alert &nbsp;·&nbsp;
                <a href="{airflow_url}">Airflow Dashboard</a>
              </div>
            </div></body></html>
            """
        )
    except Exception as e:
        print(f"Could not send skip email: {e}")


# ================================================================
# ✅ Success Notification  (data exported)
# ================================================================
def notify_on_success(dag_id, run_id, watermark_from, watermark_to,
                      total_rows, new_count, updated_count,
                      churn_count, file_path, duration_sec, log_url):
    airflow_url = _get_airflow_url()
    churn_pct   = round((churn_count / total_rows) * 100, 1) if total_rows else 0
    file_name   = os.path.basename(file_path)

    try:
        send_email(
            to=_get_recipients(),
            subject=f"✅ SUCCESS: {dag_id} — {total_rows:,} rows exported",
            html_content=f"""
            <!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">{EMAIL_STYLES}</head>
            <body><div class="wrapper">

              <div class="header" style="background:#1a1a2e;">
                <h1>✅ Churn Export Completed</h1>
                <p>Your incremental CSV is written and ready for the next pipeline stage.</p>
              </div>

              <div class="body">

                <!-- KPI Stats -->
                <div class="stat-grid">
                  <div class="stat-box">
                    <p class="num" style="color:#1a8a5a;">{total_rows:,}</p>
                    <p class="lbl">Total Rows</p>
                  </div>
                  <div class="stat-box">
                    <p class="num" style="color:#1a56db;">{new_count:,}</p>
                    <p class="lbl">New Inserts</p>
                  </div>
                  <div class="stat-box">
                    <p class="num" style="color:#e65100;">{updated_count:,}</p>
                    <p class="lbl">Updated Records</p>
                  </div>
                  <div class="stat-box">
                    <p class="num" style="color:#c0392b;">{churn_count:,}</p>
                    <p class="lbl">Churned ({churn_pct}%)</p>
                  </div>
                </div>

                <!-- Run Summary -->
                <table>
                  <tr><th colspan="2">📋 Run Summary</th></tr>
                  <tr><td><strong>DAG</strong></td><td><code>{dag_id}</code></td></tr>
                  <tr><td><strong>Run ID</strong></td><td><code>{run_id}</code></td></tr>
                  <tr><td><strong>Window From</strong></td><td>{watermark_from}</td></tr>
                  <tr><td><strong>Window To</strong></td><td>{watermark_to}</td></tr>
                  <tr><td><strong>Duration</strong></td><td>{duration_sec:.2f}s</td></tr>
                  <tr><td><strong>Status</strong></td><td><span class="badge-green">✅ Success</span></td></tr>
                </table>

                <!-- Data Breakdown -->
                <table>
                  <tr><th colspan="2">📊 Data Breakdown</th></tr>
                  <tr>
                    <td><strong>New Customers</strong></td>
                    <td>{new_count:,} <span class="badge-blue">new</span></td>
                  </tr>
                  <tr>
                    <td><strong>Updated Records</strong></td>
                    <td>{updated_count:,} <span class="badge-orange">updated</span></td>
                  </tr>
                  <tr>
                    <td><strong>Churned in batch</strong></td>
                    <td>{churn_count:,} <span class="badge-red">churn_label = Yes</span></td>
                  </tr>
                  <tr>
                    <td><strong>Churn Rate (batch)</strong></td>
                    <td><strong>{churn_pct}%</strong></td>
                  </tr>
                </table>

                <!-- File Info -->
                <table>
                  <tr><th colspan="2">📁 Output File</th></tr>
                  <tr><td><strong>File Name</strong></td><td><code>{file_name}</code></td></tr>
                  <tr><td><strong>Full Path</strong></td><td><code>{file_path}</code></td></tr>
                  <tr><td><strong>Next Watermark</strong></td><td>{watermark_to}</td></tr>
                </table>

                <a class="cta" href="{log_url}">→ View Run Logs</a>

                <p style="font-size:13px; color:#777;">
                  The watermark has been advanced to <strong>{watermark_to}</strong>.
                  The next scheduled run will pick up from this point forward.
                </p>
              </div>

              <div class="footer">
                Churn Export Pipeline Alert &nbsp;·&nbsp;
                <a href="{airflow_url}">Airflow Dashboard</a>
              </div>
            </div></body></html>
            """
        )
    except Exception as e:
        print(f"Could not send success email: {e}")


# ================================================================
# Default Args
# ================================================================
default_args = {
    'owner':               'data_engineer',
    'depends_on_past':     False,
    'start_date':          datetime(2026, 3, 6),
    'retries':             1,
    'retry_delay':         timedelta(minutes=5),
    'on_failure_callback': notify_on_failure,
}


# ================================================================
# SQL Query — Incremental via GREATEST(createdAt, updatedAt)
# ================================================================
INCREMENTAL_SQL = """
SELECT 
    u.id AS customer_id,
    COALESCE(u.gender, 'Male') AS gender,
    'No' AS senior_citizen,
    COALESCE(p.partner, 'No') AS partner,
    CASE WHEN p.dependents THEN 'Yes' ELSE 'No' END AS dependents,
    'Egypt' AS country,
    COALESCE(u.region, 'Cairo') AS state,
    COALESCE(u.region, 'Cairo') AS city,
    30753  AS zip_code,
    30.0444 AS latitude,
    31.2357 AS longitude,
    CASE WHEN p."phoneService"     THEN 'Yes' ELSE 'No' END AS phone_service,
    CASE WHEN p."multipleLines"    THEN 'Yes' ELSE 'No' END AS multiple_lines,
    COALESCE(p."internetService", 'Fiber optic')          AS internet_service,
    CASE WHEN p."onlineSecurity"   THEN 'Yes' ELSE 'No' END AS online_security,
    CASE WHEN p."onlineBackup"     THEN 'Yes' ELSE 'No' END AS online_backup,
    CASE WHEN p."deviceProtection" THEN 'Yes' ELSE 'No' END AS device_protection,
    CASE WHEN p."techSupport"      THEN 'Yes' ELSE 'No' END AS tech_support,
    CASE WHEN p."streamingTV"      THEN 'Yes' ELSE 'No' END AS streaming_tv,
    CASE WHEN p."streamingMovies"  THEN 'Yes' ELSE 'No' END AS streaming_movies,
    CASE WHEN p."paperlessBilling" THEN 'Yes' ELSE 'No' END AS paperless_billing,
    COALESCE(p."paymentMethod",  'Electronic check')      AS payment_method,
    COALESCE(p."contractType",   'Month-to-month')        AS contract,
    (
        EXTRACT(YEAR  FROM age(NOW(), u."createdAt")) * 12 +
        EXTRACT(MONTH FROM age(NOW(), u."createdAt"))
    )                                                      AS tenure_in_months,
    p."monthlyCharges"                                     AS monthly_charges_amount,
    COALESCE(
        (SELECT SUM(amount) FROM "BillingHistory" WHERE "userId" = u.id), 0
    )                                                      AS total_charges,
    CASE WHEN u.status = 'blocked' THEN 'Yes' ELSE 'No' END AS churn_label,
    CASE WHEN u.status = 'blocked' THEN '1'   ELSE '0'  END AS churn_value,
    'n/a' AS churn_score,
    'n/a' AS cltv,
    'n/a' AS churn_reason,
    u."createdAt" AS created_at,
    u."updatedAt" AS updated_at,
    CASE
        WHEN u."updatedAt" > u."createdAt" THEN 'updated'
        ELSE 'new'
    END AS record_type
FROM "User" u
LEFT JOIN "UserPersonalization" p ON u.id = p."userId"
WHERE GREATEST(u."createdAt", u."updatedAt") > %(last_watermark)s
  AND GREATEST(u."createdAt", u."updatedAt") <= %(run_timestamp)s
ORDER BY GREATEST(u."createdAt", u."updatedAt") ASC
"""


# ================================================================
# Main Function
# ================================================================
def export_data_to_csv(**context):
    ti         = context['ti']
    task_start = datetime.now()

    # ── Resolve paths ────────────────────────────────────────────────────────
    airflow_home = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')
    export_dir   = os.path.join(airflow_home, 'include', 'staging')
    os.makedirs(export_dir, exist_ok=True)

    # ── Read watermark ───────────────────────────────────────────────────────
    last_watermark = Variable.get(
        WATERMARK_VARIABLE_KEY,
        default_var=INITIAL_LOAD_DATE,
    )
    run_timestamp = task_start.strftime("%Y-%m-%d %H:%M:%S")

    print(f"Watermark window: {last_watermark} -> {run_timestamp}")

    # ── Query ────────────────────────────────────────────────────────────────
    try:
        hook = PostgresHook(postgres_conn_id='backend_postgres_db')
        df   = hook.get_pandas_df(
            INCREMENTAL_SQL,
            parameters={
                "last_watermark": last_watermark,
                "run_timestamp":  run_timestamp,
            },
        )
    except Exception as e:
        raise Exception(f"Database extraction failed: {e}")

    # ── Guard: nothing new → send SKIP email then skip ───────────────────────
    if df.empty:
        print(f"No new or updated rows since {last_watermark}. Skipping.")
        notify_on_skip(
            dag_id         = ti.dag_id,
            run_id         = ti.run_id,
            watermark_from = last_watermark,
            watermark_to   = run_timestamp,
            log_url        = ti.log_url,
        )
        raise AirflowSkipException(
            f"No new/updated data since {last_watermark} — skipping export."
        )

    # ── Compute stats ─────────────────────────────────────────────────────────
    new_count     = len(df[df['record_type'] == 'new'])
    updated_count = len(df[df['record_type'] == 'updated'])
    churn_count   = len(df[df['churn_label'] == 'Yes'])
    print(f"{len(df):,} rows — {new_count:,} new, {updated_count:,} updated, {churn_count:,} churned.")

    # ── Write CSV ────────────────────────────────────────────────────────────
    safe_from = last_watermark.replace(" ", "T").replace(":", "")
    safe_to   = run_timestamp.replace(" ", "T").replace(":", "")
    file_name = f"Churn_Export_{safe_from}_to_{safe_to}.csv"
    file_path = os.path.join(export_dir, file_name)

    try:
        df.to_csv(file_path, index=False)
        print(f"CSV written -> {file_path}")
    except Exception as e:
        raise Exception(f"Failed to write CSV: {e}")

    # ── Advance watermark ONLY after successful write ─────────────────────────
    Variable.set(WATERMARK_VARIABLE_KEY, run_timestamp)
    print(f"Watermark advanced to {run_timestamp}")

    # ── Duration ─────────────────────────────────────────────────────────────
    duration_sec = (datetime.now() - task_start).total_seconds()

    # ── Send SUCCESS email ────────────────────────────────────────────────────
    notify_on_success(
        dag_id         = ti.dag_id,
        run_id         = ti.run_id,
        watermark_from = last_watermark,
        watermark_to   = run_timestamp,
        total_rows     = len(df),
        new_count      = new_count,
        updated_count  = updated_count,
        churn_count    = churn_count,
        file_path      = file_path,
        duration_sec   = duration_sec,
        log_url        = ti.log_url,
    )

    # ── XCom ─────────────────────────────────────────────────────────────────
    ti.xcom_push(key='exported_file_path',  value=file_path)
    ti.xcom_push(key='exported_row_count',  value=len(df))
    ti.xcom_push(key='new_row_count',       value=new_count)
    ti.xcom_push(key='updated_row_count',   value=updated_count)
    ti.xcom_push(key='churn_row_count',     value=churn_count)
    ti.xcom_push(key='watermark_from',      value=last_watermark)
    ti.xcom_push(key='watermark_to',        value=run_timestamp)


# ================================================================
# DAG Definition
# ================================================================
with DAG(
    'daily_churn_export_pipeline_cloude',
    default_args=default_args,
    description='Incremental churn export — Postgres -> CSV (watermark-based)',
    schedule='@daily',
    catchup=False,
    tags=['churn_project', 'extraction', 'incremental'],
) as dag:

    export_task = PythonOperator(
        task_id='export_postgres_to_csv',
        python_callable=export_data_to_csv,
    )

    export_task