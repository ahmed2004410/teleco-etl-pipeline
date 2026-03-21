"""
Telecom Churn Prediction ML Pipelines
-------------------------------------
Provides two Airflow DAGs:
1. telecom_churn_training_monthly
2. telecom_churn_inference_daily

Real schema (discovered from DB):
    gold.fact_customer_churn  – fact_id, customer_key, contract_key,
                                payment_method_key, churn_reason_key,
                                service_key, tenure_months, monthly_charges,
                                total_charges, churn_flag, cltv,
                                churn_score, run_date
    gold.dim_customer         – customer_key, customer_id, gender,
                                senior_citizen, partner, dependents,
                                city, state
    gold.dim_contract         – contract_key, ...
    gold.dim_payment_method   – payment_method_key, ...
    gold.dim_services         – service_key, ...
    gold.churn_predictions    – output table (auto-created)

Requirements (requirements.txt):
    joblib>=1.3.0
    scikit-learn>=1.3.0
    pandas>=2.0.0
    sqlalchemy>=2.0.0
    apache-airflow-providers-common-sql>=1.8.0
    apache-airflow-providers-postgres>=5.6.0
"""

import os
import logging
from datetime import datetime, timedelta

import joblib
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID  = "churn_db_conn"
AIRFLOW_HOME      = os.getenv("AIRFLOW_HOME", "/opt/airflow")
ARTIFACTS_DIR     = os.path.join(AIRFLOW_HOME, "artifacts")
os.makedirs(ARTIFACTS_DIR, exist_ok=True)

GOLD_SCHEMA       = "gold"
FACT_TABLE        = "fact_customer_churn"
DIM_CUSTOMER      = "dim_customer"
DIM_CONTRACT      = "dim_contract"
DIM_PAYMENT       = "dim_payment_method"
DIM_SERVICES      = "dim_services"
PREDICTIONS_TABLE = "churn_predictions"

default_args = {
    "owner": "mlops",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


# ==========================================
# HELPERS
# ==========================================

def _get_versioned_paths(ds: str) -> tuple[str, str]:
    model_path   = os.path.join(ARTIFACTS_DIR, f"rf_churn_model_{ds}.pkl")
    encoder_path = os.path.join(ARTIFACTS_DIR, f"label_encoders_{ds}.pkl")
    return model_path, encoder_path


def _get_latest_artifact_paths() -> tuple[str, str]:
    model_files = sorted(
        f for f in os.listdir(ARTIFACTS_DIR)
        if f.startswith("rf_churn_model_") and f.endswith(".pkl")
    )
    encoder_files = sorted(
        f for f in os.listdir(ARTIFACTS_DIR)
        if f.startswith("label_encoders_") and f.endswith(".pkl")
    )
    if not model_files or not encoder_files:
        raise FileNotFoundError(
            f"No trained artifacts in '{ARTIFACTS_DIR}'. "
            "Run telecom_churn_training_monthly first."
        )
    latest_model   = os.path.join(ARTIFACTS_DIR, model_files[-1])
    latest_encoder = os.path.join(ARTIFACTS_DIR, encoder_files[-1])
    logger.info("Latest model   : %s", latest_model)
    logger.info("Latest encoders: %s", latest_encoder)
    return latest_model, latest_encoder


def _validate_features(X: pd.DataFrame, model: RandomForestClassifier) -> pd.DataFrame:
    expected = list(model.feature_names_in_)
    missing  = set(expected) - set(X.columns)
    if missing:
        raise ValueError(
            f"Inference data missing {len(missing)} feature(s): {sorted(missing)}"
        )
    return X[expected]


TRAINING_QUERY = f"""
    SELECT
        f.customer_key,
        dc.customer_id,
        f.run_date,
        f.tenure_months,
        f.monthly_charges,
        f.total_charges,
        f.cltv,
        f.churn_score,
        dc.gender,
        dc.senior_citizen,
        dc.partner,
        dc.dependents,
        dc.city,
        dc.state,
        CASE WHEN LOWER(f.churn_flag) IN ('yes','1','true') THEN 1 ELSE 0 END AS churn
    FROM {GOLD_SCHEMA}.{FACT_TABLE}        f
    JOIN {GOLD_SCHEMA}.{DIM_CUSTOMER}      dc  ON dc.customer_key  = f.customer_key::INTEGER
    LEFT JOIN {GOLD_SCHEMA}.{DIM_CONTRACT} dco ON dco.contract_key = f.contract_key
    LEFT JOIN {GOLD_SCHEMA}.{DIM_PAYMENT}  dp  ON dp.payment_method_key = f.payment_method_key
    LEFT JOIN {GOLD_SCHEMA}.{DIM_SERVICES} ds  ON ds.service_key   = f.service_key
"""

INFERENCE_QUERY = f"""
    SELECT
        f.customer_key,
        dc.customer_id,
        f.run_date,
        f.tenure_months,
        f.monthly_charges,
        f.total_charges,
        f.cltv,
        f.churn_score,
        dc.gender,
        dc.senior_citizen,
        dc.partner,
        dc.dependents,
        dc.city,
        dc.state
    FROM {GOLD_SCHEMA}.{FACT_TABLE}        f
    JOIN {GOLD_SCHEMA}.{DIM_CUSTOMER}      dc  ON dc.customer_key  = f.customer_key::INTEGER
    LEFT JOIN {GOLD_SCHEMA}.{DIM_CONTRACT} dco ON dco.contract_key = f.contract_key
    LEFT JOIN {GOLD_SCHEMA}.{DIM_PAYMENT}  dp  ON dp.payment_method_key = f.payment_method_key
    LEFT JOIN {GOLD_SCHEMA}.{DIM_SERVICES} ds  ON ds.service_key   = f.service_key
    WHERE f.run_date = %(run_date)s
"""


# ==========================================
# 1. MONTHLY TRAINING PIPELINE
# ==========================================

def train_churn_model(**kwargs):
    ds     = kwargs["ds"]
    hook   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    logger.info("Loading training data from gold schema ...")
    df = pd.read_sql_query(TRAINING_QUERY, con=engine)

    if df.empty:
        raise ValueError(
            "Training query returned 0 rows. "
            "Ensure gold.fact_customer_churn is populated."
        )
    logger.info("Training rows: %d", len(df))

    target_col = "churn"
    y = df[target_col]

    drop_cols = [target_col, "customer_key", "customer_id", "run_date"]
    X = df.drop(columns=drop_cols, errors="ignore").copy()
    logger.info("Feature columns: %s", list(X.columns))

    encoders    = {}
    cat_columns = X.select_dtypes(include=["object", "category"]).columns
    for col in cat_columns:
        le        = LabelEncoder()
        X[col]    = le.fit_transform(X[col].astype(str))
        encoders[col] = le

    model = RandomForestClassifier(n_estimators=100, n_jobs=-1, random_state=42)
    model.fit(X, y)

    importance = (
        pd.Series(model.feature_importances_, index=X.columns)
        .sort_values(ascending=False)
    )
    logger.info("Top 10 features:\n%s", importance.head(10).to_string())

    model_path, encoder_path = _get_versioned_paths(ds)
    joblib.dump(model,    model_path)
    joblib.dump(encoders, encoder_path)
    logger.info("Model    -> %s", model_path)
    logger.info("Encoders -> %s", encoder_path)


with DAG(
    dag_id="telecom_churn_training_monthly",
    default_args=default_args,
    description="Monthly RandomForest training on gold star schema.",
    schedule="@monthly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["mlops", "training", "telecom"],
) as training_dag:

    train_model_task = PythonOperator(
        task_id="train_rf_model",
        python_callable=train_churn_model,
    )


# ==========================================
# 2. DAILY INFERENCE PIPELINE
# ==========================================

def run_daily_inference(**kwargs):
    ds = kwargs["ds"]

    model_path, encoder_path = _get_latest_artifact_paths()
    model    = joblib.load(model_path)
    encoders = joblib.load(encoder_path)

    hook   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    chunk_iterator = pd.read_sql_query(
        INFERENCE_QUERY,
        con=engine,
        params={"run_date": ds},
        chunksize=10_000,
    )

    total_rows = 0

    for chunk in chunk_iterator:
        if chunk.empty:
            continue

        customer_ids  = chunk["customer_id"].copy()  if "customer_id"  in chunk.columns else None
        customer_keys = chunk["customer_key"].copy()  if "customer_key" in chunk.columns else None

        drop_cols = ["customer_key", "customer_id", "run_date"]
        X = chunk.drop(columns=drop_cols, errors="ignore").copy()

        for col, le in encoders.items():
            if col in X.columns:
                class_mapping = {cat: idx for idx, cat in enumerate(le.classes_)}
                X[col] = (
                    X[col]
                    .astype(str)
                    .map(class_mapping)
                    .fillna(-1)
                    .astype(int)
                )

        X = _validate_features(X, model)

        predictions = model.predict(X)
        probs       = model.predict_proba(X)[:, 1]

        out = pd.DataFrame({
            "prediction_date":   pd.to_datetime(ds).date(),
            "churn_prediction":  predictions,
            "churn_probability": probs,
        })

        if customer_ids is not None:
            out.insert(0, "customer_id",  customer_ids.values)
        if customer_keys is not None:
            out.insert(0, "customer_key", customer_keys.values)

        out.to_sql(
            name=PREDICTIONS_TABLE,
            con=engine,
            schema=GOLD_SCHEMA,
            if_exists="append",
            index=False,
        )

        total_rows += len(out)

    logger.info("Inference complete -- predictions written: %d", total_rows)


with DAG(
    dag_id="telecom_churn_inference_daily",
    default_args=default_args,
    description="Daily churn inference pipeline using gold star schema.",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["mlops", "inference", "telecom"],
) as inference_dag:

    ensure_idempotency = SQLExecuteQueryOperator(
        task_id="delete_existing_predictions_for_day",
        conn_id=POSTGRES_CONN_ID,
        sql=f"""
            DELETE FROM {GOLD_SCHEMA}.{PREDICTIONS_TABLE}
            WHERE prediction_date = '{{{{ ds }}}}';
        """,
    )

    generate_predictions = PythonOperator(
        task_id="run_batch_inference",
        python_callable=run_daily_inference,
    )

    ensure_idempotency >> generate_predictions