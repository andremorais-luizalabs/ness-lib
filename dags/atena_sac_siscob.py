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
dag = DAG('AtenaSacSiscob',
          default_args=default_args,
          schedule_interval='0 12 * * * ',
          max_active_runs=1)

# Limits of the pipeline
start = DummyOperator(task_id='Start', dag=dag)
end = DummyOperator(task_id='End', dag=dag)

# Create a Dataproc Cluster
CreateCluster = DataprocClusterCreate(dag=dag)

# Step of the SAC Importer
SacImporter = DataProcPySparkOperator(
    task_id='atena_sac_importer',
    main='gs://prd-cluster-config/etls/raw/atena/sac_importer.py',
    job_name='AtenaSacImporter',
    cluster_name=DEFAULT_CLUSTER_NAME,
    gcp_conn_id='google_cloud_default',
    delegate_to='data-engineering@maga-bigdata.iam.gserviceaccount.com',
    region='us-east1-b',
    dag=dag)

# Step of the Siscob Importer
SiscobImporter = DataProcPySparkOperator(
    task_id='atena_siscob_importer',
    main='gs://prd-cluster-config/etls/raw/atena/siscob_importer.py',
    job_name='AtenaSiscobImporter',
    cluster_name=DEFAULT_CLUSTER_NAME,
    gcp_conn_id='google_cloud_default',
    delegate_to='data-engineering@maga-bigdata.iam.gserviceaccount.com',
    region='us-east1-b',
    dag=dag)

# Delete the Dataproc Cluster
DeleteCluster = DataprocClusterDelete(dag=dag)

# Pipeline definition
start >> CreateCluster >> SacImporter
SacImporter >> SiscobImporter >> DeleteCluster >> end
