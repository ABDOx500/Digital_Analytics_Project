from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator


CLEANING_SCRIPT_PATH = "D:/Intership_Berexia/Digital_Analytics_Project/airflow/etl-scripts/November.py"
MODELING_SCRIPT_PATH = "D:/Intership_Berexia/Digital_Analytics_Project/airflow/etl-scripts/Star_Schema/Star_Schema.py"

with DAG(
    dag_id="1_python_etl_and_star_schema",
    start_date=pendulum.datetime(2025, 7, 23, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["etl", "python"],
    doc_md="""
    ### ETL and Star Schema Pipeline
    
    This DAG orchestrates the first phase of the data pipeline.
    1.  **Clean Raw Data**: Executes a Python script to clean the raw e-commerce CSV data.
    2.  **Model Star Schema**: Executes a second Python script to transform the cleaned data into a star schema format, outputting multiple CSV files.
    """,
) as dag:
    
    # Task 1: Run the Python script for cleaning the raw data
    task_clean_data = BashOperator(
        task_id="clean_raw_data",
        bash_command=f"python {CLEANING_SCRIPT_PATH}",
    )

    # Task 2: Run the Python script for modeling the star schema
    task_model_star_schema = BashOperator(
        task_id="model_star_schema",
        bash_command=f"python {MODELING_SCRIPT_PATH}",
    )

    # Define the dependency: The modeling task can only start after the cleaning task has finished successfully.
    task_clean_data >> task_model_star_schema
