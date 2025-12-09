# 🚀 Enterprise Telecom Churn Data Warehouse Pipeline
**Built with Astro CLI | Apache Airflow | Docker | Medallion Architecture**

![Astro](https://img.shields.io/badge/Astro-CLI-purple?style=flat&logo=astronomer)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9-blue?style=flat&logo=apache-airflow)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=flat&logo=docker)
![Postgres](https://img.shields.io/badge/Postgres-Data%20Warehouse-336791?style=flat&logo=postgresql)
![Status](https://img.shields.io/badge/Pipeline-Production%20Ready-green)

---

## 📖 Executive Summary

This project is a production-grade **Data Engineering Solution** designed to process high-volume Telecom customer data. Unlike standard ETL pipelines, this system features a **Self-Healing Architecture** with automated Data Quality enforcement.

The pipeline is fully containerized using **Docker** and orchestrated via **Astro CLI (Airflow)**, implementing the **Medallion Architecture** (Bronze, Silver, Gold) to transform raw logs into analytical insights (Star Schema).

---

## 🌟 Key Features & Advanced Capabilities

### 1. 🛡️ Automated Data Quality & Quarantine (The Circuit Breaker)
The pipeline does not just "fail" on bad data; it manages it intelligently:
* **Threshold-Based Validation:** If error rates exceed a defined threshold (e.g., 50 rows), the pipeline halts to prevent warehouse pollution.
* **Quarantine Logic:** Rows with specific issues (Negative Tenure, Invalid Gender, etc.) are **automatically isolated** from the clean batch.
* **Reporting:** The system generates an Excel report of rejected rows and emails it to the data steward immediately.

### 2. 🔄 "LoopBack" Reprocessing Mechanism (Correction Pipeline)
I implemented a dedicated **Event-Driven DAG** (`churn_99_reprocessing`) to handle fixed data:
* **Smart Sensors:** Continuously watches for corrected files in the `fixed_data/` directory.
* **Idempotency:** Uses `Upsert` logic (Delete + Insert) to ensure no duplicate records when re-processing data.
* **Auto-Recovery:** Once data is fixed, it automatically promotes it to Silver and refreshes the Gold layer.

### 3. 🏗️ Modern Infrastructure
* **Astro Framework:** Leveraging the modern way to run Airflow for better developer experience and deployment.
* **Dockerized Environment:** Ensures consistency across Development, Staging, and Production.
* **Modular SQL:** Transformation logic is decoupled from Python code, stored in organized `SQL/` directories for maintainability.

---

## ⚙️ Architecture & Data Flow

The project follows the **Medallion Architecture**:

| Layer | Component | Function | Technology |
| :--- | :--- | :--- | :--- |
| **Ingestion** | `load_csv_to_staging` | Detects new CSVs, creates Staging tables, archives raw files. | Python / Pandas |
| **🥉 Bronze** | `fill_bronze` | Raw data ingestion with initial tracking columns. | SQL / Postgres |
| **🥈 Silver** | `clean_silver_task` | **Complex Cleaning:** Deduplication, Type Casting, Null Handling. Bad data is moved to `include/quarantine`. | Python / SQL |
| **🥇 Gold** | `fill_gold` | Business Logic Aggregation. Creates Fact & Dimension tables (**Star Schema**). | SQL (Data Marts) |

---

## 🛠️ Tech Stack

* **Orchestration:** Apache Airflow (via Astro CLI)
* **Language:** Python 3.9 (Pandas, SQLAlchemy)
* **Database:** PostgreSQL (Local Data Warehouse)
* **Containerization:** Docker & Docker Compose
* **Alerting:** SMTP (Gmail Relay) for failure & quality alerts.
* **Testing:** `pytest` for DAG integrity & logic validation.

---

## 📂 Project Structure

```text
TELECO-ETL-PIPELINE/
│
├── dags/
│   ├── DataWarehouse.py           # 🚀 Main Daily ETL Pipeline
│   ├── Reprocessing.py            # 🔄 Event-Driven Fix Pipeline
│   └── SQL/                       # Modular SQL Scripts
│       ├── Bronze/                # Raw DDLs & Inserts
│       ├── Silver/                # Cleaning Logic
│       └── Gold/                  # Fact/Dim Creation
│
├── include/
│   ├── staging/                   # Landing zone for new files
│   ├── quarantine/                # ⚠️ Automated rejected data landing
│   ├── fixed_data/                # 📥 Drop zone for corrected files
│   └── archive/                   # Historical raw data
│
├── tests/                         # CI/CD Tests
│   └── test_dag_integrity.py      # Ensures no cyclic dependencies
│
├── Dockerfile                     # Astro Runtime Image
├── packages.txt                   # OS dependencies
└── requirements.txt               # Python libs (Pandas, Postgres, etc.)
```
---
##🔔 Monitoring & Alerts
**⚠️ On Failure**

* **Instant email alert including:**

* **DAG ID**

* **Task ID**

* **Error Log (Stack Trace)**

* **🚫 On Data-Quality Rejection**

* **Auto-generated Excel Report**

* **Automatically emailed to the Operations Team**

* **Includes detailed reason for every rejected record
(e.g., Missing ID, Negative Tenure, Invalid Gender)**

---

##🚀 How to Run

###1️⃣ Clone & Start
git clone https://github.com/YourUsername/Telecom-ETL-Pipeline.git
cd Telecom-ETL-Pipeline
astro dev start

---

###2️⃣ Access Airflow

Open your browser at:

👉 http://localhost:8080

Login credentials:

Username: admin

Password: admin

---

###3️⃣ Trigger the Pipeline

Place your source CSV file into:

include/staging/


Enable the DAG:

---

###➡️ Data_Warehouse_Full_Pipeline

Then simply sit back and watch the magic happen ✨

##👨‍💻 Author

**Ahmed Anwer Fath**
Data Engineer
