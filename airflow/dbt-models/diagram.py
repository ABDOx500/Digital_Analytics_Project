from diagrams import Diagram, Cluster, Edge
from diagrams.gcp.analytics import BigQuery
from diagrams.gcp.storage import GCS
from diagrams.programming.language import Python
from diagrams.programming.framework import React, Spring
from diagrams.onprem.analytics import Dbt
from diagrams.onprem.workflow import Airflow
from diagrams.generic.device import Tablet
from diagrams.generic.storage import Storage as GenericStorage
from diagrams.digitalocean.compute import Docker

# Define custom graph attributes for a professional and clean look
graph_attr = {
    "bgcolor": "white",
    "pad": "0.5",
    "splines": "ortho",
    "nodesep": "0.8",
    "ranksep": "1.2",
    "fontname": "Arial",
    "fontsize": "12",
}

# Define custom node attributes for consistency
node_attr = {
    "fontname": "Arial",
    "fontsize": "10",
    "shape": "box",
    "style": "rounded",
}

# Define custom edge attributes
edge_attr = {
    "color": "#4B5563",
    "style": "solid",
    "fontsize": "8",
}

with Diagram(
    "Digital Analytics Pipeline - Containerized Architecture", 
    filename="digital_analytics_pipeline_architecture", 
    show=False, 
    graph_attr=graph_attr, 
    node_attr=node_attr, 
    edge_attr=edge_attr
):
    
    # Define the end user
    user = Tablet("Business User")

    # --- Phase 1 & 2: Data Pipeline ---
    with Cluster("Automated Data Pipeline"):
        data_source = GenericStorage("E-commerce Data\n(Raw CSV)")

        with Cluster("Dockerized Orchestration Environment"):
            # The entire automated workflow runs inside Docker
            docker_container = Docker("Docker Container")
            
            with Cluster("Airflow Orchestration"):
                orchestrator = Airflow("Airflow Scheduler")
                
                # Tasks are defined within the Airflow context
                python_etl = Python("Python ETL Task")
                dbt_run = Dbt("dbt Build Task")

                orchestrator >> Edge(label="triggers") >> python_etl
                python_etl >> Edge(label="then triggers") >> dbt_run

        with Cluster("Google Cloud Platform"):
            gcs_bucket = GCS("Google Cloud Storage")
            
            with Cluster("BigQuery Data Warehouse"):
                source_tables = BigQuery("Source Dataset\n(Nov_star_sample)")
                target_tables = BigQuery("Target KPI Tables\n(dbt_aezzari)")

    # --- Phase 3: Analytics & Serving ---
    with Cluster("Phase 3: Analytics & Serving (Containerized)"):
        with Cluster("Docker Environment"):
            backend_api = Spring("Spring Boot Backend")
            frontend_dashboard = React("React Frontend")
        
        backend_api >> Edge(label="provides data to") >> frontend_dashboard
        frontend_dashboard >> user

    # --- Define Main Data Flow ---
    data_source >> python_etl
    python_etl >> Edge(label="outputs CSVs to") >> gcs_bucket
    gcs_bucket >> Edge(label="loads into") >> source_tables
    dbt_run >> Edge(label="reads from Source, writes to Target") >> source_tables
    dbt_run - Edge(color="transparent") - target_tables # Invisible edge for layout
    target_tables >> Edge(label="is read by") >> backend_api

