"""
Clinical Batch Processing DAG – Phase 2
Pain negation + symptom extraction + stable ETL pipeline.
"""

from datetime import datetime, timedelta
import logging
import re

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger("clinical_batch_processing_dag")

DEFAULT_ARGS = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30),
}

# -------------------------------------------
# NLP LOGIC (Phase 2)
# -------------------------------------------

NEGATION_PATTERNS = [
    r"no pain",
    r"denies pain",
    r"without pain",
    r"pain free",
    r"pain-free",
    r"absence of pain",
    r"free of pain",
]

SYMPTOMS = [
    "fever",
    "cough",
    "vomiting",
    "headache",
    "nausea",
    "weakness",
    "fatigue",
    "dizziness",
    "swelling",
    "chest pain",
    "shortness of breath",
    "diarrhea",
    "abdominal pain",
    "leg pain",
    "back pain",
]


def detect_pain_negation(text: str) -> int:
    """
    Return 1 if true pain is present, 0 if negated or absent.
    """
    if not text:
        return 0

    t = text.lower()

    # No "pain" word at all -> no pain
    if "pain" not in t:
        return 0

    # "pain" word exists; check for negation phrases
    for pattern in NEGATION_PATTERNS:
        if re.search(pattern, t):
            return 0

    # Real pain
    return 1


def extract_symptoms(text: str):
    """
    Return list of symptoms detected in the note_text.
    """
    if not text:
        return []
    t = text.lower()

    found = []
    for s in SYMPTOMS:
        if s in t:
            found.append(s)

    return found


# -------------------------------------------
# DAG TASK FUNCTIONS
# -------------------------------------------


def _validate_and_get_params(**kwargs):
    ctx = get_current_context()
    dag_run = ctx.get("dag_run")
    conf = dag_run.conf if dag_run else {}

    sql_input_table = conf.get("sql_input_table")
    sql_output_table = conf.get("sql_output_table")
    lower_limit = conf.get("lower_limit")
    upper_limit = conf.get("upper_limit")
    pg_conn_id = conf.get("pg_conn_id") or "postgres_default"

    if not sql_input_table or not sql_output_table:
        raise ValueError("sql_input_table and sql_output_table are required")
    if lower_limit is None or upper_limit is None:
        raise ValueError("lower_limit and upper_limit are required")

    lower_limit = int(lower_limit)
    upper_limit = int(upper_limit)
    if lower_limit > upper_limit:
        raise ValueError("lower_limit cannot be greater than upper_limit")

    # Validate table names against SQL injection
    for tbl in (sql_input_table, sql_output_table):
        if not re.match(r"^[A-Za-z0-9_]+$", tbl):
            raise ValueError(f"Invalid table name: {tbl}")

    params = {
        "sql_input_table": sql_input_table,
        "sql_output_table": sql_output_table,
        "lower_limit": lower_limit,
        "upper_limit": upper_limit,
        "pg_conn_id": pg_conn_id,
        "run_id": dag_run.run_id if dag_run else None,
    }
    log.info("Params: %s", params)

    return params


def _record_run_start(**kwargs):
    ti = kwargs["ti"]
    params = ti.xcom_pull(task_ids="validate_and_get_params")

    hook = PostgresHook(postgres_conn_id=params["pg_conn_id"])

    hook.run(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id SERIAL PRIMARY KEY,
            dag_run_id TEXT,
            input_table TEXT,
            output_table TEXT,
            lower_limit BIGINT,
            upper_limit BIGINT,
            start_ts TIMESTAMP DEFAULT now(),
            end_ts TIMESTAMP,
            status TEXT,
            rows_processed INTEGER
        );
        """
    )

    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO pipeline_runs (
            dag_run_id, input_table, output_table,
            lower_limit, upper_limit, status, rows_processed
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """,
        (
            params["run_id"],
            params["sql_input_table"],
            params["sql_output_table"],
            params["lower_limit"],
            params["upper_limit"],
            "RUNNING",
            0,
        ),
    )
    audit_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()

    ti.xcom_push(key="audit_id", value=audit_id)


def _fetch_rows(**kwargs):
    ti = kwargs["ti"]
    params = ti.xcom_pull(task_ids="validate_and_get_params")

    hook = PostgresHook(postgres_conn_id=params["pg_conn_id"])
    sql = f"""
        SELECT unique_id, note_text
        FROM {params['sql_input_table']}
        WHERE unique_id >= %s AND unique_id <= %s
        ORDER BY unique_id;
    """

    rows = hook.get_records(
        sql, parameters=(params["lower_limit"], params["upper_limit"])
    )
    log.info("Fetched %d rows", len(rows))
    return rows


def _process_rows(**kwargs):
    ti = kwargs["ti"]
    rows = ti.xcom_pull(task_ids="fetch_rows") or []

    processed = []
    for uid, txt in rows:
        text = txt or ""

        summary = text[:200]
        pain_flag = detect_pain_negation(text)
        symptoms = extract_symptoms(text)

        # store symptoms as string (e.g. "['fever', 'cough']")
        processed.append((uid, summary, pain_flag, str(symptoms)))

    log.info("Processed %d rows", len(processed))
    return processed


def _write_results_atomic(**kwargs):
    ti = kwargs["ti"]
    params = ti.xcom_pull(task_ids="validate_and_get_params")
    processed = ti.xcom_pull(task_ids="process_rows") or []
    audit_id = ti.xcom_pull(task_ids="record_run_start", key="audit_id")

    hook = PostgresHook(postgres_conn_id=params["pg_conn_id"])
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        # Create table if missing
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {params['sql_output_table']} (
                unique_id BIGINT PRIMARY KEY,
                summary TEXT,
                flag INTEGER,
                symptoms TEXT
            );
            """
        )
        # Ensure 'symptoms' column exists even if table came from Phase 1
        cur.execute(
            f"ALTER TABLE {params['sql_output_table']} "
            "ADD COLUMN IF NOT EXISTS symptoms TEXT;"
        )

        if processed:
            upsert_sql = f"""
                INSERT INTO {params['sql_output_table']}
                (unique_id, summary, flag, symptoms)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (unique_id) DO UPDATE SET
                    summary = EXCLUDED.summary,
                    flag = EXCLUDED.flag,
                    symptoms = EXCLUDED.symptoms;
            """
            cur.executemany(upsert_sql, processed)

        cur.execute(
            """
            UPDATE pipeline_runs
            SET status = %s, end_ts = now(), rows_processed = %s
            WHERE id = %s;
            """,
            ("SUCCESS", len(processed), audit_id),
        )

        conn.commit()

    except Exception:
        conn.rollback()
        cur.execute(
            "UPDATE pipeline_runs SET status = %s, end_ts = now() WHERE id = %s;",
            ("FAILED", audit_id),
        )
        conn.commit()
        log.exception("Failed to write results; audit_id=%s marked FAILED", audit_id)
        raise

    finally:
        cur.close()
        conn.close()


# -------------------------------------------
# DAG DEFINITION
# -------------------------------------------

with DAG(
    dag_id="clinical_batch_processing_dag",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=8,
    tags=["clinical", "processing", "phase2"],
) as dag:

    validate_and_get_params = PythonOperator(
        task_id="validate_and_get_params",
        python_callable=_validate_and_get_params,
    )

    record_run_start = PythonOperator(
        task_id="record_run_start",
        python_callable=_record_run_start,
    )

    fetch_rows = PythonOperator(
        task_id="fetch_rows",
        python_callable=_fetch_rows,
    )

    process_rows = PythonOperator(
        task_id="process_rows",
        python_callable=_process_rows,
    )

    write_results_atomic = PythonOperator(
        task_id="write_results_atomic",
        python_callable=_write_results_atomic,
    )

    validate_and_get_params >> record_run_start >> fetch_rows >> process_rows >> write_results_atomic
