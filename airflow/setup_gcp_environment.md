# GCP Environment Setup Guide

## Prerequisites for Full Pipeline

### 1. Google Cloud Project Setup

1. **Create or select a GCP project**
2. **Enable required APIs:**
   ```bash
   gcloud services enable bigquery.googleapis.com
   gcloud services enable storage.googleapis.com
   ```

### 2. Create Service Account

1. **Create service account:**
   ```bash
   gcloud iam service-accounts create airflow-dbt-sa \
     --display-name="Airflow dbt Service Account"
   ```

2. **Grant required permissions:**
   ```bash
   # BigQuery permissions
   gcloud projects add-iam-policy-binding digital-analytics-464705 \
     --member="serviceAccount:airflow-dbt-sa@digital-analytics-464705.iam.gserviceaccount.com" \
     --role="roles/bigquery.dataEditor"
   
   gcloud projects add-iam-policy-binding digital-analytics-464705 \
     --member="serviceAccount:airflow-dbt-sa@digital-analytics-464705.iam.gserviceaccount.com" \
     --role="roles/bigquery.jobUser"

   # Storage permissions  
   gcloud projects add-iam-policy-binding digital-analytics-464705 \
     --member="serviceAccount:airflow-dbt-sa@digital-analytics-464705.iam.gserviceaccount.com" \
     --role="roles/storage.objectAdmin"
   ```

3. **Download service account key:**
   ```bash
   gcloud iam service-accounts keys create dbt-user-creds.json \
     --iam-account=airflow-dbt-sa@digital-analytics-464705.iam.gserviceaccount.com
   ```

### 3. Create GCS Bucket

```bash
gsutil mb -l africa-south1 gs://star_shcema
```

### 4. Update Configuration Files

1. **Update `main_etl_pipeline.py`:**
   - Change `BIGQUERY_PROJECT = "your-gcp-project-id"` to your actual project ID
   - Optionally change `GCS_BUCKET_NAME` if you used a different bucket name

2. **Update `profiles.yml`:**
   - Change `project: your-gcp-project-id` to your actual project ID

3. **Update `docker-compose.yaml`:**
   - Ensure the credentials file path is correct:
     ```yaml
     - /path/to/your/dbt-user-creds.json:/opt/airflow/dbt-user-creds.json:ro
     ```

### 5. Set Environment Variable

Add to your `.env` file (or set in docker-compose.yaml):
```bash
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/dbt-user-creds.json
```

## Testing the Pipeline

### 1. Test Individual Components

```bash
# Test data processing
python /opt/airflow/etl_scripts/main_etl_pipeline.py process

# Test GCS upload  
python /opt/airflow/etl_scripts/main_etl_pipeline.py upload

# Test BigQuery load
python /opt/airflow/etl_scripts/main_etl_pipeline.py load

# Test dbt build
cd /opt/airflow/dbt_project && dbt build --profiles-dir .
```

### 2. Run Full Pipeline

Use the Airflow UI to trigger the `full_digital_analytics_pipeline` DAG.

## Expected Results

After successful execution:

1. **GCS Bucket**: `gs://star_shcema/star_schema/` contains 6 CSV files
2. **BigQuery Dataset**: `Nov_star_sample` contains 6 source tables (in africa-south1)
3. **BigQuery Dataset**: `dbt_aezzari` contains transformed KPI tables (in africa-south1)
4. **dbt Documentation**: Available via `dbt docs generate && dbt docs serve`

## Troubleshooting

### Common Issues:

1. **Permission Denied**: Check service account permissions
2. **Bucket Not Found**: Verify bucket name and location
3. **BigQuery Dataset Issues**: Check project ID and dataset names
4. **dbt Connection Issues**: Verify profiles.yml configuration 