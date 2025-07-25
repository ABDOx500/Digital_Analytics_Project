from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

TEST_SCRIPT_PATH = "/opt/airflow/etl_scripts/test.py"

with DAG(
    dag_id="0_hello_world_test",
    start_date=pendulum.datetime(2025, 7, 24, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:

    test_task = BashOperator(
    task_id="run_test_script",
    bash_command=f"python {TEST_SCRIPT_PATH}",
    retries=1,
    retry_delay=pendulum.duration(minutes=5),
)