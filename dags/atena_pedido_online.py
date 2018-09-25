from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from sness.utils.slack_utils import slack_failed_task
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from sness.config.config import DEFAULT_CLUSTER_NAME
from sness.airflow_utils.airflow_utils import DataprocClusterCreate, DataprocClusterDelete

default_args = {
    'owner': 'Data Engineering',
    'depends_on_past': False,
    'description': 'Move os arquivos do bucket Transient do Atena para o bucket Raw transformando-os em parquet via pyspark.',
    'schedule_interval': '30 * * * *',
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2018, 9, 8),
    'on_failure_callback': slack_failed_task}

dag = DAG('AtenaPedidoOnline', default_args=default_args)

inicio = DummyOperator(task_id='Inicio', dag=dag)
fim = DummyOperator(task_id='Fim', dag=dag)

CreateCluster = DataprocClusterCreate(dag)

OnlineOrder = DataProcPySparkOperator(
    task_id='online_pedido_transient_to_raw',
    main='gs://prd-cluster-config/etls/raw/atena/online-pedido.py',
    job_name='AtenaOnlineOrderTransientToRaw',
    cluster_name=DEFAULT_CLUSTER_NAME,
    gcp_conn_id='google_cloud_default',
    delegate_to='data-engineering@maga-bigdata.iam.gserviceaccount.com',
    region='us-east1-b',
    dag=dag)


DeleteCluster = DataprocClusterDelete(dag)

inicio >> CreateCluster >> OnlineOrder >> DeleteCluster >> fim
