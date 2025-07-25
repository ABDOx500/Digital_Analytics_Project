#!/usr/bin/env python3
"""
Main ETL Pipeline Script for Digital Analytics Project

This script handles the complete data pipeline:
- process: Run data cleaning and star schema creation
- upload: Upload CSV files to Google Cloud Storage
- load: Load data from GCS to BigQuery tables

Usage:
    python main_etl_pipeline.py process
    python main_etl_pipeline.py upload  
    python main_etl_pipeline.py load
"""

import sys
import os
import subprocess
from pathlib import Path
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd

# Configuration
GCS_BUCKET_NAME = "star_shcema"  # GCS bucket name
BIGQUERY_DATASET = "Nov_star_sample"  # source dataset
BIGQUERY_PROJECT = "digital-analytics-464705"  # GCP project ID

# Paths (container paths)
ETL_SCRIPTS_DIR = "/opt/airflow/etl_scripts"
STAR_SCHEMA_DIR = f"{ETL_SCRIPTS_DIR}/Star_Schema"
CLEANED_DATA_DIR = f"{ETL_SCRIPTS_DIR}/Cleaned data"

# CSV files to upload/load
STAR_SCHEMA_FILES = [
    "dim_date.csv",
    "dim_session.csv", 
    "dim_product.csv",
    "dim_user.csv",
    "dim_event_type.csv",
    "fact_events.csv"
]

def run_data_processing():
    """Run the data cleaning and star schema creation scripts"""
    print("🔄 Starting data processing...")
    
    # Step 1: Run data cleaning
    print("📊 Running data cleaning (November.py)...")
    cleaning_script = f"{ETL_SCRIPTS_DIR}/November.py"
    result = subprocess.run(["python", cleaning_script], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Data cleaning failed: {result.stderr}")
        sys.exit(1)
    else:
        print("✅ Data cleaning completed successfully")
    
    # Step 2: Run star schema creation  
    print("🌟 Running star schema creation (Star_Schema.py)...")
    star_schema_script = f"{STAR_SCHEMA_DIR}/Star_Schema.py"
    result = subprocess.run(["python", star_schema_script], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Star schema creation failed: {result.stderr}")
        sys.exit(1)
    else:
        print("✅ Star schema creation completed successfully")

def upload_to_gcs():
    """Upload CSV files to Google Cloud Storage"""
    print("☁️ Starting upload to Google Cloud Storage...")
    
    try:
        # Initialize GCS client
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        
        # Upload star schema files
        for filename in STAR_SCHEMA_FILES:
            local_path = f"{STAR_SCHEMA_DIR}/{filename}"
            if os.path.exists(local_path):
                # Upload to GCS with path structure
                gcs_path = f"star_schema/{filename}"
                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(local_path)
                print(f"✅ Uploaded {filename} to gs://{GCS_BUCKET_NAME}/{gcs_path}")
            else:
                print(f"⚠️ File not found: {local_path}")
        
        # Also upload the cleaned data file
        cleaned_file = f"{CLEANED_DATA_DIR}/Cleaned_Nov.csv"
        if os.path.exists(cleaned_file):
            blob = bucket.blob("cleaned_data/Cleaned_Nov.csv")
            blob.upload_from_filename(cleaned_file)
            print(f"✅ Uploaded cleaned data to GCS")
            
        print("🎉 All files uploaded to GCS successfully!")
        
    except Exception as e:
        print(f"❌ GCS upload failed: {str(e)}")
        sys.exit(1)

def load_to_bigquery():
    """Load data from GCS to BigQuery tables"""
    print("🏗️ Starting load to BigQuery...")
    
    try:
        # Initialize BigQuery client
        client = bigquery.Client(project=BIGQUERY_PROJECT)
        
        # Create dataset if it doesn't exist
        dataset_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}"
        try:
            client.get_dataset(dataset_id)
            print(f"✅ Dataset {BIGQUERY_DATASET} already exists")
        except:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "africa-south1"
            client.create_dataset(dataset, timeout=30)
            print(f"✅ Created dataset {BIGQUERY_DATASET}")
        
        # Define table schemas (simplified - BigQuery will auto-detect)
        for filename in STAR_SCHEMA_FILES:
            table_name = filename.replace('.csv', '')
            table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{table_name}"
            
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,  # Skip header row
                autodetect=True,  # Auto-detect schema
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite
            )
            
            # GCS URI
            uri = f"gs://{GCS_BUCKET_NAME}/star_schema/{filename}"
            
            # Start load job
            load_job = client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )
            
            # Wait for job to complete
            load_job.result()
            
            # Get table info
            table = client.get_table(table_id)
            print(f"✅ Loaded {table.num_rows} rows into {table_name}")
        
        print("🎉 All tables loaded to BigQuery successfully!")
        
    except Exception as e:
        print(f"❌ BigQuery load failed: {str(e)}")
        sys.exit(1)

def main():
    """Main function to handle command line arguments"""
    if len(sys.argv) != 2:
        print("Usage: python main_etl_pipeline.py {process|upload|load}")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "process":
        run_data_processing()
    elif command == "upload":
        upload_to_gcs()
    elif command == "load":
        load_to_bigquery()
    else:
        print(f"❌ Unknown command: {command}")
        print("Valid commands: process, upload, load")
        sys.exit(1)

if __name__ == "__main__":
    main() 