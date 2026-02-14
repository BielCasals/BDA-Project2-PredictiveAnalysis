## Project2 BDA- Predictive Analysis
Project focused on building an end-to-end data workflow: from raw data ingestion and necessary treatment to model training, evaluation, and orchestration using data engineering and machine learning tools.

The pipeline uses: 
Spark + MongoDB + Delta + Airflow + MLflow/Optuna as the pipeline to predict Barcelona neighborhood income index (`RDF_index`) using open data on density, immigration, unemployment, and income.


### How the pipeline is structured
- **Raw inputs**: CSV/JSON under `Datasets/` (income, density, immigrants, unemployment, lookup_tables, others). Adjust the path in [config.py](config.py) if your raw data lives elsewhere.
- **Landing zone**: `landingzone/` stores ingested batches organized per category.
- **Formatted zone**: `formattedzone/` stores cleaned JSON and writes the same data to MongoDB (`formattedzone` database).
- **Exploitation zone**: `exploitationzone/` holds Delta tables with engineered features ready for modeling.
- **Orchestration**: [my_airflow/dags/project_workflow.py](my_airflow/dags/project_workflow.py) runs the collection → formatting → transformation → ML training.

### 🚧 Planned Improvements
- Enable task-level parallelism using Airflow operators and Spark optimizations
- Add retry policies and failure handling in DAG definitions
- Externalize configuration using environment variables or config files
- Introduce monitoring and alerting for pipeline failures

### Requirements
- Python 3.10+ with Java 8+ on the machine (needed by Spark).
- MongoDB running locally on `127.0.0.1:27017` (change in [config.py](config.py) if different).
- Apache Spark via PySpark (pinned in [requirements.txt](requirements.txt)).
- MLflow UI (defaults to port `5000`).

Install Python libraries (better to do it on a venv named `de_env` to match the scripts):
```bash
python -m venv de_venv
source de_venv/bin/activate
pip install -r requirements.txt
```

### Configure paths
- Update `PATHS["datasets"]` in [config.py](config.py) to the folder that holds your raw data (current default assumes `Datasets/datasets`, while the repo ships data under `Datasets/`).
- Set `PROJECT_ROOT` in [my_airflow/dags/config_airflow.py](my_airflow/dags/config_airflow.py) to your absolute project path.
- If using Airflow, point `AIRFLOW_HOME` to `my_airflow/` so it picks up the DAG.


### Use of Airflow 
1) Ensure `PROJECT_ROOT` is set in `config_airflow.py` and `AIRFLOW_HOME` points to `my_airflow/`.
2) Initialize Airflow and start the services (UI on port 8080):
```bash
airflow db init
airflow webserver --port 8080 &
airflow scheduler &
```
3) Enable the `project_workflow` DAG; it will run the four tasks in order.

### Service helpers (for Linux/WSL)
- `start_project.sh` starts MongoDB, activates `de_venv`, launches MLflow UI on 5000, and starts Airflow.
- `stop_project.sh` stops Airflow/MLflow and MongoDB. These scripts use `bash`.

### Modeling details
- Target: `RDF_index` (relative income index per neighborhood/year).
- Features: `net_density`, `ratio_immigrants`, `population`, `ratio_unemployed` from the exploitation Delta table.
- Models compared with Optuna: Linear Regression, Random Forest, Gradient Boosted Trees. Best model retrained and logged to MLflow; predictions exported to `predictions.csv`.

### Data flow per script
- [datacollector.py](datacollector.py): Copies new raw files into dated `landingzone` batches and tracks processed files per category.
- [dataformatter.py](dataformatter.py): Cleans/normalizes each domain dataset, writes JSON to `formattedzone`, and persists to MongoDB collections (`income`, `density`, `immigrants`, `unemployment`, `names_lookup`).
- [datatransformer.py](datatransformer.py): Reads from MongoDB, joins datasets, engineers ratios, and saves Delta output to `exploitationzone`.
- [ml_pipeline_v2.py](ml_pipeline_v2.py): Loads Delta data, trains/tunes models with Optuna, logs to MLflow, and saves predictions.



