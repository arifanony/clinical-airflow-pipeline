"""
Clinical Batch DAG
Splits range into batches and triggers clinical_batch_processing_dag for each batch.
"""

from datetime import datetime, timedelta
import logging
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger("clinical_batch_dag")

DEFAULT_ARGS = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id="clinical_batch_dag",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["clinical", "batching"],
)
def clinical_batch_dag():
    @task()
    def validate_and_compute_batches():
        from airflow.operators.python import get_current_context
        ctx = get_current_context()
        conf = ctx.get("dag_run").conf if ctx.get("dag_run") else {}

        sql_input_table = conf.get("sql_input_table")
        sql_output_table = conf.get("sql_output_table")
        lower = conf.get("lower_limit")
        upper = conf.get("upper_limit")
        batch_size = conf.get("batch_size", 1000)
        pg_conn_id = conf.get("pg_conn_id", "postgres_default")

        if not sql_input_table or not sql_output_table:
            raise ValueError("sql_input_table and sql_output_table must be provided")
        if lower is None or upper is None:
            raise ValueError("lower_limit and upper_limit must be provided")

        lower, upper, batch_size = int(lower), int(upper), int(batch_size)
        if lower > upper or batch_size <= 0:
            raise ValueError("Invalid range or batch_size")

        ranges = []
        cur = lower
        while cur <= upper:
            end = min(cur + batch_size - 1, upper)
            ranges.append({"lower_limit": cur, "upper_limit": end})
            cur = end + 1

        batch_confs = [
            {
                "sql_input_table": sql_input_table,
                "sql_output_table": sql_output_table,
                "lower_limit": r["lower_limit"],
                "upper_limit": r["upper_limit"],
                "pg_conn_id": pg_conn_id,
            }
            for r in ranges
        ]

        log.info("Computed %d batch ranges", len(batch_confs))
        return batch_confs

    batch_confs = validate_and_compute_batches()

    TriggerDagRunOperator.partial(
        task_id="trigger_clinical_batches",
        trigger_dag_id="clinical_batch_processing_dag",
        wait_for_completion=False,
        reset_dag_run=False,
    ).expand(conf=batch_confs)

clinical_batch_dag()