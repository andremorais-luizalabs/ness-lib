from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from sness.utils.slack_utils import slack_failed_task
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from sness.config.config import DEFAULT_CLUSTER_NAME
from sness.utils.aiflow_utils import DataprocClusterCreate, DataprocClusterDelete

default_args = {
    'owner': 'Data Engineering',
    'depends_on_past': False,
    'description': 'Move os arquivos do bucket Transient do Atena para o bucket Raw transformando-os em parquet via pyspark.',
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2018, 9, 14),
    'on_failure_callback': slack_failed_task,}

dag = DAG('AtenaSingleCustomer', default_args=default_args, schedule_interval='* 5 * * * ')

inicio = DummyOperator(task_id='Inicio', dag=dag)
fim = DummyOperator(task_id='Fim', dag=dag)

CreateCluster = DataprocClusterCreate(dag)

OnlineCustomer = DataProcPySparkOperator(
    task_id='atena_online_customer',
    main='gs://prd-cluster-config/etls/raw/atena/online_customers.py',
    job_name='AtenaOnlineCustomer',
    cluster_name=DEFAULT_CLUSTER_NAME,
    gcp_conn_id='google_cloud_default',
    delegate_to='data-engineering@maga-bigdata.iam.gserviceaccount.com',
    region='us-east1-b',
    dag=dag)


GemcoCustomer = DataProcPySparkOperator(
    task_id='atena_gemco_customer',
    main='gs://prd-cluster-config',
    job_name='AtenaGemcoCustomer',
    cluster_name=DEFAULT_CLUSTER_NAME,
    gcp_conn_id='google_cloud_default',
    delegate_to='data-engineering@maga-bigdata.iam.gserviceaccount.com',
    region='us-east1-b',
    dag=dag)


SingleCustomer = DataProcPySparkOperator(
    task_id='atena_single_customer',
    main='gs://prd-cluster-config/etls/trusted/atena/customer_dedup.py',
    job_name='AtenaSingleCustomer',
    cluster_name=DEFAULT_CLUSTER_NAME,
    gcp_conn_id='google_cloud_default',
    delegate_to='data-engineering@maga-bigdata.iam.gserviceaccount.com',
    region='us-east1-b',
    dag=dag)



DeleteCluster = DataprocClusterDelete(dag)

inicio.set_downstream(CreateCluster)
CreateCluster.set_downstream([OnlineCustomer, GemcoCustomer])
SingleCustomer.set_upstream([OnlineCustomer, GemcoCustomer])
DeleteCluster.set_upstream(SingleCustomer)
fim.set_upstream(DeleteCluster)