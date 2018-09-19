import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator

from sness.utils.airflow_utils import DataprocClusterCreate, DataprocClusterDelete
from sness.utils.slack_utils import slack_failed_task
from sness.config.config import DEFAULT_CLUSTER_NAME
from sness.gcloud import gs_to_bq

dag_default_args = {
    'owner': 'Dados Intelligence',
    'description': 'Move os arquivos do bucket Transient da Precifica para o bucket Raw transformando-os em parquet via pyspark.',
    'start_date': datetime.datetime(2018, 9, 15),
    'schedule_interval': '10 * * * *',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': datetime.timedelta(seconds=10),
    'on_failure_callback': slack_failed_task
}

dag = DAG('Precifica',
          max_active_runs=1,
          default_args=dag_default_args,
          catchup=False
          )

StartDummy = DummyOperator(task_id='Start', dag=dag)

# DownloadCSVPrice = SFTPOperator(
#     task_id="download_csv_price",
#     ssh_hook=SSHHook(ssh_conn_id='ssh_precifica'),
#     local_filepath=tmp_file,
#     remote_filepath=sftp_file_price,
#     operation=SFTPOperation.GET,
#     dag=dag
# )
#
# SendPriceToGCS = FileToGoogleCloudStorageOperator(
#     task_id='send_price_to_transient',
#     src=tmp_file,
#     dst=bucket_transient_folder + "/" + final_file,
#     bucket=bucket_transient,
#     google_cloud_storage_conn_id='gcp',
#     mime_type='text/csv',
#     dag=dag
# )

CreateCluster = DataprocClusterCreate(dag)

Price = DataProcPySparkOperator(
    task_id='price_transient_to_raw',
    main='gs://prd-cluster-config/etls/raw/precifica/price.py',
    job_name='PrecificaPriceTransientToRaw',
    cluster_name=DEFAULT_CLUSTER_NAME,
    gcp_conn_id='google_cloud_default',
    delegate_to='data-engineering@maga-bigdata.iam.gserviceaccount.com',
    region='us-east1-b',
    dag=dag)


# To Do: alterar o prefix (segundo parametro) concatenando a data variavel.
# >>> 'price_new/partition_date='+datetime.now().strftime("%Y-%m-%d")
def gs_to_bq_context():

    gs_to_bq.run_import(
        bucket='prd-lake-raw-precifica',
        prefix='price_new/partition_date=2018-09-14',
        dataset='pricing',
        table='precifica_price'
    )


SendPriceToBq = PythonVirtualenvOperator(
    task_id='send_price_to_big_query',
    python_callable=gs_to_bq_context,
    requirements=['google-api-python-client==1.7.4', 'google-api-core==1.3.0', 'google-cloud==0.34.0',
                  'google-cloud-storage==1.11.0', 'google-cloud-bigquery==1.5.0', 'pandas==0.23.4',
                  'scikit-learn==0.19.2', 'fire==0.1.3'],
    python_version='2.7',
    dag=dag)

DeleteCluster = DataprocClusterDelete(dag)

EndDummy = DummyOperator(task_id='End', dag=dag)

StartDummy >> CreateCluster >> Price >> SendPriceToBq >> DeleteCluster >> EndDummy
