"""
movianalytics_etl_dag.py
==========================
Orchestrates the full MoviAnalytics pipeline:

    extract_movielens ─┐
                        ├─> transform_spark ─> load_postgres ─> validate
    extract_fred_cpi  ──┘

Scheduled daily; each task shells out to the corresponding etl/*.py script
so the same code can also be run standalone outside Airflow for local dev.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "movianalytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

ETL_DIR = "/opt/airflow/etl"

with DAG(
    dag_id="movianalytics_etl",
    description="MoviAnalytics: MovieLens + FRED CPI -> PySpark -> Postgres star schema",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["movianalytics", "etl"],
) as dag:

    extract_movielens = BashOperator(
        task_id="extract_movielens",
        bash_command=f"python {ETL_DIR}/extract_movielens.py",
    )

    extract_fred_cpi = BashOperator(
        task_id="extract_fred_cpi",
        bash_command=f"python {ETL_DIR}/extract_fred_cpi.py",
    )

    transform_spark = BashOperator(
        task_id="transform_spark",
        bash_command=f"python {ETL_DIR}/transform_spark.py",
    )

    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command=f"python {ETL_DIR}/load_postgres.py",
        env={"AIRFLOW_RUN_ID": "{{ run_id }}"},
        append_env=True,
    )

    validate = BashOperator(
        task_id="validate",
        bash_command=f"python {ETL_DIR}/validate.py",
        env={"AIRFLOW_RUN_ID": "{{ run_id }}"},
        append_env=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    [extract_movielens, extract_fred_cpi] >> transform_spark >> load_postgres >> validate
