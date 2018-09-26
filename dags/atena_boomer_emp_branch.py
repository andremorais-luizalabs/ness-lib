import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from sness.config.config import DEFAULT_CLUSTER_NAME
from sness.utils.slack_utils import slack_failed_task
from sness.airflow_utils.airflow_utils import DataprocClusterCreate, \
    DataprocClusterDelete

# Default Arguments
default_args = {
    'owner': 'Data Engineering',
    'depends_on_past': False,
    'description': '''Moves Sac and Siscob Informations
    from transient to raw in Parket format.''',
    'retries': 5,
    'retry_delay': datetime.timedelta(seconds=10),
    'start_date': datetime.datetime(2018, 9, 26),
    'on_failure_callback': slack_failed_task,
}

# Dag definition
dag = DAG('AtenaBoomerEmpBranch',
          default_args=default_args,
          schedule_interval='0 6 * * * ',
          max_active_runs=1)

# Limits of the pipeline
start = DummyOperator(task_id='Start', dag=dag)
end = DummyOperator(task_id='End', dag=dag)

# Create a Dataproc Cluster
CreateCluster = DataprocClusterCreate(dag=dag)

# Step of the SAC Importer
BoomerangImporter = DataProcPySparkOperator(
    task_id='atena_Boomerang_importer',
    main='gs://prd-cluster-config/etls/raw/atena/boomerang_importer.py',
    job_name='AtenaBoomerangImporter',
    cluster_name=DEFAULT_CLUSTER_NAME,
    gcp_conn_id='google_cloud_default',
    delegate_to='data-engineering@maga-bigdata.iam.gserviceaccount.com',
    region='us-east1-b',
    dag=dag)

# Step of the Siscob Importer
EmployeeBranchImporter = DataProcPySparkOperator(
    task_id='atena_employee_branch_importer',
    main='gs://prd-cluster-config/etls/raw/atena/employee_branch_importer.py',
    job_name='AtenaEmployeeBranchImporter',
    cluster_name=DEFAULT_CLUSTER_NAME,
    gcp_conn_id='google_cloud_default',
    delegate_to='data-engineering@maga-bigdata.iam.gserviceaccount.com',
    region='us-east1-b',
    dag=dag)

# Delete the Dataproc Cluster
DeleteCluster = DataprocClusterDelete(dag)

# Pipeline definition
start >> CreateCluster >> BoomerangImporter
BoomerangImporter >> EmployeeBranchImporter >> DeleteCluster >> end
