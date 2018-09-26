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
    'description': 'Moves GC Customers from transient to raw in Parket format',
    'retries': 5,
    'retry_delay': datetime.timedelta(seconds=10),
    'start_date': datetime.datetime(2018, 9, 26),
    'on_failure_callback': slack_failed_task,
}

# Dag definition
dag = DAG('AtenaGcCustomer',
          default_args=default_args,
          schedule_interval='0 20 * * * ',
          max_active_runs=1)

# Limits of the pipeline
start = DummyOperator(task_id='Start', dag=dag)
end = DummyOperator(task_id='End', dag=dag)

# Create a Dataproc Cluster
CreateCluster = DataprocClusterCreate(dag=dag)

# Step of the GC-Customer Importer
GcCustomerImporter = DataProcPySparkOperator(
    task_id='atena_gc_customer_importer',
    main='gs://prd-cluster-config/etls/raw/atena/gc_customer_importer.py',
    job_name='AtenaGcCustomerImporter',
    cluster_name=DEFAULT_CLUSTER_NAME,
    gcp_conn_id='google_cloud_default',
    delegate_to='data-engineering@maga-bigdata.iam.gserviceaccount.com',
    region='us-east1-b',
    dag=dag)

# Delete the Dataproc Cluster
DeleteCluster = DataprocClusterDelete(dag=dag)

# Pipeline definition
start >> CreateCluster >> GcCustomerImporter >> DeleteCluster >> end
