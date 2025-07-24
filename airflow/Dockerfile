# Dockerfile

# Start with the exact same Airflow version as your docker-compose.yaml file
FROM apache/airflow:2.9.2

# Switch to the default airflow user (good practice)
USER airflow

# Copy your requirements file into the image
COPY requirements.txt /

# Run pip install to add your project's Python packages
RUN pip install --no-cache-dir -r /requirements.txt

# Copy your ETL scripts into the container
# This assumes your etl_scripts folder is in the same directory as your Dockerfile
COPY etl-scripts/ /opt/airflow/etl_scripts/

# Optional: Set permissions (if needed)
# USER root
# RUN chown -R airflow:airflow /opt/airflow/etl_scripts/
# USER airflow