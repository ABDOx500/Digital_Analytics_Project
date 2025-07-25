# Dockerfile

# Airflow version of docker-compose.yaml file
FROM apache/airflow:2.9.2

# Switch to the default airflow user 
USER airflow

# Copying the requirements file into the image
COPY requirements.txt /

# Run pip install to add the project's Python packages
RUN pip install --no-cache-dir -r /requirements.txt


# This assumes the etl_scripts folder is in the same directory as the Dockerfile
COPY etl-scripts/ /opt/airflow/etl_scripts/

