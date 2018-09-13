from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Data Engineering',
    'depends_on_past': False,
    'description': 'Move os arquivos do bucket Transient do Atena para o bucket Raw transformando-os em parquet via pyspark.',
    'schedule_interval': 'H/5 * * * *',
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2018, 9, 8)
}

dag = DAG('make_lib', default_args=default_args)

inicio = DummyOperator(task_id='Inicio', dag=dag)
fim = DummyOperator(task_id='Fim', dag=dag)

uploadLibGcp= BashOperator(
    task_id = "upload_Lib_Airflow",
    bash_command = "git clone https://github.com/luizalabs/ness-lib.git && cd ness-lib && gsutil -m cp -r sness/ gs://us-east1-ness-maestro-782d5135-bucket/dags/",
    dag=dag)

uploadDagsGcp= BashOperator(
    task_id = "upload_dags_Airflow",
    bash_command = "git clone https://github.com/luizalabs/ness-lib.git && cd ness-lib && gsutil -m cp -r dags/ gs://us-east1-ness-maestro-782d5135-bucket/dags/",
    dag=dag)


inicio.set_downstream([uploadDagsGcp, uploadLibGcp])
fim.set_upstream([uploadDagsGcp, uploadLibGcp])