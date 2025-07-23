# Dockerfile

# Start with the exact same Airflow version as your docker-compose.yaml file
FROM apache/airflow:2.9.2

# Switch to the default airflow user (good practice)
USER airflow

# Copy your requirements file into the image
COPY requirements.txt /

# Run pip install to add your project's Python packages
RUN pip install --no-cache-dir -r /requirements.txt