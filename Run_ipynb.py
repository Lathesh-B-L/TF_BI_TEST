from datetime import datetime, timedelta
from airflow import DAG, models
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator
from airflow.operators.bash_operator import BashOperator


#from datetime import datetime, timedelta
#from airflow import DAG, models
#from airflow.lineage import AUTO
#from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator
#from airflow.operators.bash_operator import BashOperator
#from airflow.operators.python import PythonOperator
#from airflow.providers.papermill.operators.papermill import PapermillOperator
#from airflow.utils.dates import days_ago



# Airflow parameters, see https://airflow.incubator.apache.org/code.html
DEFAULT_DAG_ARGS = {
    'owner': 'airflow',  # The owner of the task.
    # Task instance should not rely on the previous task's schedule to succeed.
    'depends_on_past': False,
    # We use this in combination with schedule_interval=None to only trigger the DAG with a
    # POST to the REST API.
    # Alternatively, we could set this to yesterday and the dag will be triggered upon upload to the
    # dag folder.
    'start_date': datetime.utcnow(),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,  # Retry once before failing the task.
    'retry_delay': timedelta(minutes=5),  # Time between retries.
    'project_id': 'maximal-muse-251207',  # Cloud Composer project ID.
    # We only want the DAG to run when we POST to the api.
    # Alternatively, this could be set to '@daily' to run the job once a day.
    # more options at https://airflow.apache.org/scheduler.html#dag-runs
    'schedule_interval': None
}


with DAG('DATAPROC-JOBS-RUN_IPYNB',
         default_args=DEFAULT_DAG_ARGS) as dag:
	create_dataproc_cluster = DataprocClusterCreateOperator(
		project_id = 'maximal-muse-251207',
		task_id = 'create_dataproc_cluster', 
		cluster_name = 'cluster-3316',
		num_workers = 2,
		master_machine_type = 'n1-standard-2',
		region = 'us-west1',
		image_version = '1.3-deb9',
		master_disk_size = 100,
		worker_machine_type = 'n1-standard-2',
		zone = 'us-west1-a'
	)
	
	submit_spark_job_term = BashOperator(
        task_id='CS_D_ADAPPL_CNTR_ipynb',
        bash_command="gcloud dataproc jobs submit spark --cluster cluster-3316 --region us-west1 --jar gs://sjsu_cdw/notebooks/jupyter/Full_Load/Admissions/CS_D_ADAPPL_CNTR.ipynb"
    )	
		
	create_dataproc_cluster >> submit_spark_job_term 
	
	


	#create_dataproc_cluster.dag = dag