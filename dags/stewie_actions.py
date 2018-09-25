import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from sness.utils.airflow_utils import DataprocClusterCreate, DataprocClusterDelete
from sness.utils import slack_utils, date_utils
from sness.config import config
from sness.gcloud import gs_to_bq

dag_default_args = {
    'owner': 'Dados Recomendacoes',
    'description': 'Pipeline de ingestao de actions do contexto Stewie',
    'start_date': datetime.datetime(2018, 9, 20),
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': datetime.timedelta(seconds=10),
    'on_failure_callback': slack_utils.slack_failed_task
}

dag = DAG('ActionsPipeline',
          schedule_interval='10 * * * *',
          max_active_runs=1,
          default_args=dag_default_args,
          catchup=False
          )

StartDummy = DummyOperator(task_id='Start', dag=dag)

CreateCluster = DataprocClusterCreate(dag)

TransientToRaw = DataProcPySparkOperator(
    task_id='actions_transient_to_raw',
    main='gs://prd-cluster-config/etls/raw/stewie/actions_transient_to_raw.py',
    job_name='ActionsTransientToRaw',
    cluster_name=config.DEFAULT_CLUSTER_NAME,
    gcp_conn_id='google_cloud_default',
    delegate_to='data-engineering@maga-bigdata.iam.gserviceaccount.com',
    region='us-east1-b',
    dag=dag)

RawToBigQuery = PythonOperator(
    task_id='send_to_big_query',
    python_callable=gs_to_bq.run_import,
    op_args=['prd-lake-raw-stewie', date_utils.build_today_partition('actions/day_interacted_at='), 'stewie', 'actions'],
    provide_context=False,
    dag=dag
)

DeleteCluster = DataprocClusterDelete(dag)

EndDummy = DummyOperator(task_id='End', dag=dag)

StartDummy >> CreateCluster >> TransientToRaw >> RawToBigQuery >> DeleteCluster >> EndDummy
