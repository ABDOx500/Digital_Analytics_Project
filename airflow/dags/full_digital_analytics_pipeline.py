from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator


DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
ETL_SCRIPT_PATH = "/opt/airflow/etl_scripts/main_etl_pipeline.py"

with DAG(
    dag_id="full_digital_analytics_pipeline",
    start_date=pendulum.datetime(2025, 7, 25, tz="UTC"),
    schedule=None,  # Manual trigger 
    catchup=False,
    tags=["ecommerce", "dbt", "production", "gcp", "bigquery"],
    doc_md="""
    ### Full Digital Analytics Pipeline
    
    This DAG orchestrates the entire end-to-end data pipeline:
    1.  **Process & Model**: Cleans the raw data and transforms it into a star schema, saving the output as local CSV files.
    2.  **Upload to GCS**: Uploads the star schema CSVs to a Google Cloud Storage bucket.
    3.  **Load to BigQuery**: Loads the data from GCS into the source tables in BigQuery.
    4.  **Install dbt Dependencies**: Downloads required dbt packages (dbt_utils).
    5.  **Run dbt Build**: Triggers the dbt project to run all transformations, tests, and create the final KPI tables.
    
    **Prerequisites:**
    - Google Cloud credentials configured (GOOGLE_APPLICATION_CREDENTIALS)
    - GCS bucket created: star_shcema (africa-south1)
    - BigQuery project: digital-analytics-464705 (africa-south1)
    
    **Data Flow:**
    Raw CSV → Cleaned Data → Star Schema → GCS → BigQuery → dbt KPIs
    """,
) as dag:

    # Task 1: Process the raw data and create the star schema CSVs
    task_process_data = BashOperator(
        task_id="process_and_model_data",
        bash_command=f"python {ETL_SCRIPT_PATH} process",
        doc_md="""
        Runs the complete data processing pipeline:
        - November.py: Data cleaning and feature engineering
        - Star_Schema.py: Dimensional modeling and CSV generation
        
        Output: 6 CSV files (5 dimensions + 1 fact table)
        """,
    )

    # Task 2: Upload the processed CSVs to Google Cloud Storage
    task_upload_to_gcs = BashOperator(
        task_id="upload_csvs_to_gcs",
        bash_command=f"python {ETL_SCRIPT_PATH} upload",
        doc_md="""
        Uploads all star schema CSV files to Google Cloud Storage.
        
        Target: gs://star_shcema/star_schema/
        Files: dim_*.csv, fact_events.csv
        """,
    )

    # Task 3: Load the data from GCS into BigQuery source tables
    task_load_to_bigquery = BashOperator(
        task_id="load_gcs_to_bigquery",
        bash_command=f"python {ETL_SCRIPT_PATH} load",
        doc_md="""
        Loads CSV data from GCS into BigQuery tables.
        
        Target Dataset: Nov_star_sample
        Tables: dim_date, dim_session, dim_product, dim_user, dim_event_type, fact_events
        """,
    )

    # Task 4: Install dbt dependencies
    task_dbt_deps = BashOperator(
        task_id="install_dbt_dependencies",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir .",
        doc_md="""
        Installs dbt package dependencies.
        
        Packages: dbt_utils (for generate_surrogate_key, etc.)
        """,
    )

    # Task 5: Run the dbt build command to transform data in BigQuery
    task_dbt_build = BashOperator(
        task_id="run_dbt_build",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt build --profiles-dir .",
        doc_md="""
        Executes the complete dbt workflow:
        - dbt run: Execute all model transformations
        - dbt test: Run all data quality tests
        
        Output: KPI tables in dbt_aezzari dataset
        Models: Star_Schema, conversion_rate, daily_summary, sales metrics, etc.
        """,
    )

    # Define the dependencies to create the full, sequential workflow
    task_process_data >> task_upload_to_gcs >> task_load_to_bigquery >> task_dbt_deps >> task_dbt_build 