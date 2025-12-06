from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger("clinical_parent_dag")

DEFAULT_ARGS = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

def _build_conf(**kwargs):
    dag_run = kwargs.get("dag_run")
    incoming = dag_run.conf if dag_run and dag_run.conf else {}

    default_conf = {
        "sql_input_table": "clinical_notes_input",
        "sql_output_table": "clinical_notes_output",
        "lower_limit": 1,
        "upper_limit": 10,
        "batch_size": 3,
        "pg_conn_id": "postgres_default"
    }

    # merge UI config on top of defaults
    final_conf = {**default_conf, **incoming}

    log.info("Final merged config: %s", final_conf)

    # ⭐ convert dict → JSON string
    return json.dumps(final_conf)


with DAG(
    dag_id="clinical_parent_dag",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=False,  # ⭐ must be False
    tags=["clinical", "parent"],
) as dag:

    build_conf = PythonOperator(
        task_id="build_conf",
        python_callable=_build_conf,
    )

    trigger_batch = TriggerDagRunOperator(
        task_id="trigger_clinical_batch_dag",
        trigger_dag_id="clinical_batch_dag",
        wait_for_completion=False,
        reset_dag_run=False,
        # ⭐ pass JSON string, not dict
        conf="{{ ti.xcom_pull(task_ids='build_conf') }}"
    )

    build_conf >> trigger_batch
