from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator, SlackAPIOperator
from sness.gcloud.dataproc import get_client, list_clusters, create_cluster, wait_for_cluster_creation
from sness.config import config
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator, DataprocClusterCreateOperator
from sness.utils.slack_utils import slack_failed_task
DEFAULT_CLUSTER_NAME = "prd-cluster-jobs"


default_args = {
    'owner': 'Data Engineering',
    'depends_on_past': False,
    'description': 'Move os arquivos do bucket Transient do Atena para o bucket Raw transformando-os em parquet via pyspark.',
    'schedule_interval': '30 * * * *',
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2018, 9, 8),
    'on_failure_callback': slack_failed_task}

dag = DAG('AtenaTransientToRaw', default_args=default_args)

inicio = DummyOperator(task_id='Inicio', dag=dag)
fim = DummyOperator(task_id='Fim', dag=dag)


def cluster_creation():
    gclient = get_client()
    cluster_list = list_clusters(gclient, config.PROJECT, config.REGION)
    if DEFAULT_CLUSTER_NAME not in cluster_list:
        create_cluster(gclient, config.PROJECT, config.ZONE, config.REGION, DEFAULT_CLUSTER_NAME)
        wait_for_cluster_creation(gclient, config.PROJECT, config.REGION, DEFAULT_CLUSTER_NAME)


ClusterStep = PythonOperator(
            task_id='Cluster_creation',
            python_callable=cluster_creation)


OnlineOrder = DataProcPySparkOperator(
    task_id='online_pedido_transient_to_raw',
    main='gs://prd-cluster-config/pyspark/raw/actions_transient_to_raw.py',
    job_name='AtenaOnlineOrderTransientToRaw',
    cluster_name=DEFAULT_CLUSTER_NAME,
    gcp_conn_id='google_cloud_default',
    delegate_to='data-engineering@maga-bigdata.iam.gserviceaccount.com',
    region='us-east1-b',
    dag=dag)


inicio >> ClusterStep >> OnlineOrder >> fim
