from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from sness.config.config import DEFAULT_CLUSTER_NAME
from sness.utils.slack_utils import slack_failed_task
from sness.airflow_utils.airflow_utils import DataprocClusterCreate, DataprocClusterDelete


# Default Arguments
default_args = {
    'owner': 'Data Engineering',
    'depends_on_past': False,
    'description': '''Moves sales orders from transient to raw in Parket format,
                      Calcutes the purchase profile for Mobile Vendas.''',
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2018, 9, 25),
    'on_failure_callback': slack_failed_task, }

# Dag definition
dag = DAG('AtenaOrdersPurchaseProfile',
          default_args=default_args,
          schedule_interval='0 6 * * * ',
          max_active_runs=1)

# Limits of the pipeline
start = DummyOperator(task_id='Start', dag=dag)
end = DummyOperator(task_id='End', dag=dag)

# Create a Dataproc Cluster
CreateCluster = DataprocClusterCreate(dag=dag)


# Step of the Products Importer
ErpProductImporter = DataProcPySparkOperator(
    task_id='atena_erp_product_importer',
    main='gs://prd-cluster-config/etls/raw/atena/erp_product_importer.py',
    job_name='AtenaErpProductImporter',
    cluster_name=DEFAULT_CLUSTER_NAME,
    gcp_conn_id='google_cloud_default',
    delegate_to='data-engineering@maga-bigdata.iam.gserviceaccount.com',
    region='us-east1-b',
    dag=dag)

# Step of the Orders Importer
ErpOrderImporter = DataProcPySparkOperator(
    task_id='atena_erp_order_importer',
    main='gs://prd-cluster-config/etls/raw/atena/erp_order_importer.py',
    job_name='AtenaErpOrderImporter',
    cluster_name=DEFAULT_CLUSTER_NAME,
    gcp_conn_id='google_cloud_default',
    delegate_to='data-engineering@maga-bigdata.iam.gserviceaccount.com',
    region='us-east1-b',
    dag=dag)

# Step of the Calculate of the Purchase Profile
PurchaseProfile = DataProcPySparkOperator(
    task_id='atena_purchase_profile_mobven',
    main='gs://prd-cluster-config/etls/trusted/' +
    'atena/purchase_profile_mobilevendas.py',
    job_name='AtenaPurchaseProfileMobVen',
    cluster_name=DEFAULT_CLUSTER_NAME,
    gcp_conn_id='google_cloud_default',
    delegate_to='data-engineering@maga-bigdata.iam.gserviceaccount.com',
    region='us-east1-b',
    dag=dag)


# Delete the Dataproc Cluster
DeleteCluster = DataprocClusterDelete(dag=dag)

# Pipeline definition
start >> CreateCluster >> ErpProductImporter >> ErpOrderImporter
ErpOrderImporter >> PurchaseProfile >> DeleteCluster >> end
