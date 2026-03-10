"""
Churn Prediction ML Model
=========================
Trains an XGBoost classifier on Gold layer data to predict customer churn.
Saves predictions and probabilities to gold.churn_predictions.

Compatible with Airflow 2.9. Callable from PythonOperator.
"""

from datetime import datetime

import pandas as pd
import xgboost as xgb
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sklearn.preprocessing import LabelEncoder, StandardScaler


def train_and_predict_churn(conn_id: str = "churn_db_conn") -> None:
    """
    Extract Gold data, train XGBoost classifier, and save churn predictions.

    Args:
        conn_id: Airflow Postgres connection ID (default: churn_db_conn).

    Raises:
        ValueError: If insufficient data for training.
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    # -------------------------------------------------------------------------
    # 1. Extract joined Gold data (fact + dimensions)
    # -------------------------------------------------------------------------
    extract_sql = """
    SELECT
        f.customer_key,
        c.customer_id,
        f.tenure_months,
        f.monthly_charges,
        f.total_charges,
        f.churn_flag,
        COALESCE(ct.contract_type, 'Unknown') AS contract_type,
        COALESCE(pm.payment_method, 'Unknown') AS payment_method,
        COALESCE(c.gender, 'Unknown') AS gender,
        COALESCE(c.senior_citizen, '0') AS senior_citizen,
        COALESCE(c.partner, 'No') AS partner,
        COALESCE(c.dependents, 'No') AS dependents,
        COALESCE(s.internet_service, 'Unknown') AS internet_service,
        COALESCE(s.phone_service, 'No') AS phone_service,
        COALESCE(s.online_security, 'No') AS online_security,
        COALESCE(s.streaming_tv, 'No') AS streaming_tv
    FROM gold.fact_customer_churn f
    JOIN gold.dim_customer c ON c.customer_key = f.customer_key
    LEFT JOIN gold.dim_contract ct ON ct.contract_key = f.contract_key
    LEFT JOIN gold.dim_payment_method pm ON pm.payment_method_key = f.payment_method_key
    LEFT JOIN gold.dim_services s ON s.service_key = f.service_key
    WHERE f.tenure_months IS NOT NULL
      AND f.monthly_charges IS NOT NULL
      AND f.churn_flag IN ('0', '1')
    """

    df = pd.read_sql(extract_sql, engine)

    if len(df) < 10:
        raise ValueError(
            f"Insufficient data for training: {len(df)} rows. Need at least 10."
        )

    # -------------------------------------------------------------------------
    # 2. Target and feature columns
    # -------------------------------------------------------------------------
    target_col = "churn_flag"
    y = (df[target_col].astype(str).str.strip() == "1").astype(int)

    categorical_cols = [
        "contract_type",
        "payment_method",
        "gender",
        "senior_citizen",
        "partner",
        "dependents",
        "internet_service",
        "phone_service",
        "online_security",
        "streaming_tv",
    ]
    numeric_cols = ["tenure_months", "monthly_charges", "total_charges"]

    # -------------------------------------------------------------------------
    # 3. Encode categorical variables
    # -------------------------------------------------------------------------
    X_cat = pd.DataFrame(index=df.index)
    label_encoders = {}

    for col in categorical_cols:
        if col not in df.columns:
            continue
        le = LabelEncoder()
        vals = df[col].fillna("Unknown").astype(str)
        X_cat[col] = le.fit_transform(vals)
        label_encoders[col] = le

    # -------------------------------------------------------------------------
    # 4. Scale numeric features
    # -------------------------------------------------------------------------
    X_num = df[numeric_cols].fillna(0)
    scaler = StandardScaler()
    X_num_scaled = pd.DataFrame(
        scaler.fit_transform(X_num),
        columns=numeric_cols,
        index=df.index,
    )

    X = pd.concat([X_num_scaled, X_cat], axis=1)

    # -------------------------------------------------------------------------
    # 5. Train XGBoost Classifier
    # -------------------------------------------------------------------------
    model = xgb.XGBClassifier(
        n_estimators=100,
        max_depth=4,
        learning_rate=0.1,
        random_state=42,
        use_label_encoder=False,
        eval_metric="logloss",
    )
    model.fit(X, y)

    # Predict and get probabilities
    y_pred = model.predict(X)
    y_prob = model.predict_proba(X)[:, 1]  # P(churn=1)

    # -------------------------------------------------------------------------
    # 6. Save to gold.churn_predictions (full refresh per run)
    # -------------------------------------------------------------------------
    run_ts = datetime.now()
    results = pd.DataFrame(
        {
            "customer_key": df["customer_key"],
            "customer_id": df["customer_id"],
            "churn_prediction": y_pred,
            "churn_probability": y_prob,
            "model_run_date": run_ts,
        }
    )

    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE gold.churn_predictions RESTART IDENTITY"))
        results.to_sql(
            "churn_predictions",
            conn,
            schema="gold",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=500,
        )

    print(
        f"Churn model trained on {len(df)} rows. "
        f"Predictions saved to gold.churn_predictions. "
        f"Churn rate (predicted): {y_pred.mean():.2%}"
    )
