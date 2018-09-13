from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from sness.utils.slack_utils import slack_failed_task

default_args = {
    'owner': 'Data Engineering',
    'depends_on_past': False,
    'description': 'Move os arquivos de ETL para o bucket de config.',
    'schedule_interval': '30 * * * *',
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2018, 9, 8),
    'on_failure_callback': slack_failed_task,}

dag = DAG('UploadEtls', default_args=default_args)

inicio = DummyOperator(task_id='Inicio', dag=dag)
fim = DummyOperator(task_id='Fim', dag=dag)

copy = BashOperator(task_id='copy_to_bucket',
             bash_command="git clone https://github.com/luizalabs/ness-lib.git && cd ness-lib && gsutil -m cp -r etls/ gs://prd-cluster-config/",
             dag=dag)

inicio >> copy >> fim
