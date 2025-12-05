"""
Mangages the workflow using Airflow
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

PROJECT_PATH = "/mnt/c/Users/bielc/OneDrive/Documentos/UPC/Q5/BDA/Projecte2-PredictiveAnalytics"

env_config = {
    "PYTHONPATH": PROJECT_PATH,
    "SPARK_LOCAL_IP": "127.0.0.1"
}


with DAG(
    dag_id="project_workflow",
    start_date=datetime(2025,1,1),
    schedule_interval="@daily",
    catchup=False
)as dag:
    #collect data
    t1_collect = BashOperator(
        task_id='Data_Collection',
        bash_command=f"python3 {PROJECT_PATH}/datacollector.py"
        #env=env_config  # Pass the environment!

    )

    #format data
    t2_format = BashOperator(
        task_id="Data_Formatting",
        bash_command=f"python3 {PROJECT_PATH}/dataformatter.py"
        #env=env_config  # Pass the environment!
    )

    #transform data
    t3_transform = BashOperator(
        task_id="Data_Transformation",
        bash_command=f"python3 {PROJECT_PATH}/datatransformer.py"
        #env=env_config  # Pass the environment!
    )

    # ml pipeline
    t4_mlprocess = BashOperator(
        task_id="ML_pipeline",
        bash_command=f"python3 {PROJECT_PATH}/ml_pipeline.py"
        #env=env_config  # Pass the environment!

    )

    #Dependencies
    t1_collect >> t2_format >> t3_transform >> t4_mlprocess