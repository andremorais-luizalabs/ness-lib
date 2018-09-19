from datetime import datetime

from airflow import DAG
from airflow import models
from airflow.operators.mysql_operator import MySqlOperator

from sness.utils.slack_utils import slack_failed_task

dag_default_args = {
    'owner': 'Data Engineering',
    'description': 'Deleta do Web UI as DAGs removidas do diretório de DAGSs do composer: '
                   'Usar a variavel de ambinete "afDagID" para informar a DAG a ser deletada',
    'start_date': datetime(2018, 9, 15),
    'schedule_interval': None,
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': datetime.timedelta(seconds=10),
    'on_failure_callback': slack_failed_task
}

dag = DAG(
    'CleanDeletedDags',
    default_args=dag_default_args,
    catchup=False
)

DeleteXComOperator = MySqlOperator(
  task_id='delete-xcom-record-task',
  mysql_conn_id='airflow_db',
  sql="DELETE from xcom where dag_id='{}'".format(models.Variable.get('afDagID')),
  dag=dag)

DeleteTaskOperator = MySqlOperator(
  task_id='delete-task-record-task',
  mysql_conn_id='airflow_db',
  sql="DELETE from task_instance where dag_id='{}'".format(models.Variable.get('afDagID')),
  dag=dag)

DeleteSLAMissOperator = MySqlOperator(
  task_id='delete-sla-record-task',
  mysql_conn_id='airflow_db',
  sql="DELETE from sla_miss where dag_id='{}'".format(models.Variable.get('afDagID')),
  dag=dag)

DeleteLogOperator = MySqlOperator(
  task_id='delete-log-record-task',
  mysql_conn_id='airflow_db',
  sql="DELETE from log where dag_id='{}'".format(models.Variable.get('afDagID')),
  dag=dag)

DeleteJobOperator = MySqlOperator(
  task_id='delete-job-record-task',
  mysql_conn_id='airflow_db',
  sql="DELETE from job where dag_id='{}'".format(models.Variable.get('afDagID')),
  dag=dag)

DeleteDagRunOperator = MySqlOperator(
  task_id='delete-dag_run-record-task',
  mysql_conn_id='airflow_db',
  sql="DELETE from dag_run where dag_id='{}'".format(models.Variable.get('afDagID')),
  dag=dag)

DeleteDagOperator = MySqlOperator(
  task_id='delete-dag-record-task',
  mysql_conn_id='airflow_db',
  sql="DELETE from dag where dag_id='{}'".format(models.Variable.get('afDagID')),
  dag=dag)

DeleteXComOperator >> DeleteTaskOperator >> DeleteSLAMissOperator >> DeleteLogOperator >> DeleteJobOperator >> DeleteDagRunOperator >> DeleteDagOperator
