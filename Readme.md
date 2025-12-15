## Data Analysis

The files are given structured, where the root contains all the scripts, where the dag will search for all the spark scripts. 
The dag is located at my_airflow/dags/, with a config_airflow.py file which has to be modified by the user to locate the project root on the user system.

For the user commodity we have designed a shell script start_project.sh that initiates mongod, the mlflow user interface at port:5000 and the airflow scheduler and the user interface at port:8080
If not it can be automatically initiated by making the same steps but manually.

We also defined a shell script to stop all the system running at each port we initiated.. 
