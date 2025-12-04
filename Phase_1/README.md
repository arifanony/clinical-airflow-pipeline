# 🚀 Phase 1 – Core ETL + Multi-DAG Pipeline Architecture (Days took - 5)

Phase 1 focuses on building a production-style clinical ETL pipeline using **Apache Airflow** and **PostgreSQL**.  
The goal was to design a stable, scalable workflow capable of batching, processing, logging, and preparing data for future AI/NLP phases.

---

## 🧱 What I Built in Phase 1

### **1️⃣ Parent DAG – `clinical_parent_dag`**
- Accepts runtime configuration.
- Triggers the Batch DAG.
- Acts as the entry point for the entire pipeline.

### **2️⃣ Batch DAG – `clinical_batch_dag`**
- Splits large numeric ID ranges into multiple batches.
- Generates dynamic configs.
- Uses Airflow `.expand()` for parallel triggering.
- Sends each batch to the Processing DAG.

### **3️⃣ Processing DAG – `clinical_batch_processing_dag`**
- Validates input parameters.
- Fetches rows from PostgreSQL safely.
- Applies initial NLP logic (pain detection).
- Handles negations (“no pain”, “denies pain”, “pain free”).
- Writes results to `clinical_notes_output`.
- Logs each run in `pipeline_runs` with:
  - status  
  - timestamps  
  - rows_processed  
- Uses atomic writes + rollback for safety.

---
<img width="1759" height="669" alt="image" src="https://github.com/user-attachments/assets/3ee2608b-ad7d-45d9-a653-32a94cf4f0e5" />


<img width="1186" height="424" alt="image" src="https://github.com/user-attachments/assets/62142ed2-c293-41da-bbc4-8fdec988482a" />


## 🏆 Technical Achievements of Phase 1

- Designed a clean **multi-DAG architecture**.
- Implemented dynamic batching.
- Connected Airflow with PostgreSQL using hooks.
- Added database audit logging.
- Created a Dockerized Airflow environment.
- Enabled API authentication to trigger DAGs with config.
- Ensured stable task execution using `PythonOperator`.

---

## ⚠️ Challenges I Faced in Phase 1

### **1. Missing “Trigger with Config” button in Airflow UI**
Fixed by enabling API auth backends:

AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.session,airflow.api.auth.backend.basic_auth

---

### **2. JSONDecodeError when triggering DAGs**
Cause: Bad config formatting.  
Fix: Pass valid JSON-only structures.

---

### **3. Postgres authentication failures**
Airflow expected a `postgres` user, but container created an `airflow` user.  
Fixed by unifying credentials:

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow


### **4. PostgresHook missing provider**
Airflow failed to load provider modules.  
Fix:

_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-postgres


### **5. Context errors with TaskFlow API**
Solution: rewrite Processing DAG using classic `PythonOperator` for stability.

---

## 🎯 Phase 1 Outcome

Phase 1 successfully delivered a production-ready ETL pipeline featuring:

- Dynamic batching  
- Multi-DAG orchestration  
- NLP-ready processing  
- Audit logging  
- Safe Postgres transactions  
- Fully Dockerized Airflow environment  

This phase creates a strong foundation for Phase 2, where machine learning and Transformer-based NLP models will be integrated into the pipeline.



