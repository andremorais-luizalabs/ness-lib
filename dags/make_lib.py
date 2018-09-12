from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Data Engineering',
    'depends_on_past': False,
    'description': 'Move os arquivos do bucket Transient do Atena para o bucket Raw transformando-os em parquet via pyspark.',
    'schedule_interval': '30 9 * * *',
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2018, 9, 8)
}

dag = DAG('AtenaTransientToRaw', default_args=default_args)

inicio = DummyOperator(task_id='Inicio', dag=dag)
fim = DummyOperator(task_id='Fim', dag=dag)
pull_and_make = BashOperator(
    task_id = "pull_and_make_lib",
    bash_command = "git clone https://github.com/luizalabs/ness-lib.git && cd ness-lib && make",
    dag=dag)


install_lib = BashOperator(
    task_id = "install_lib",
    bash_command = "pip install dist/sness-0.0.1-py2-none-any.whl",
    dag=dag)


inicio >> pull_and_make >> install_lib >> fim