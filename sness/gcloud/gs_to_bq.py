import os
import logging
import sys
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

'''
Example run
python pispark.py --bucket prd-lake-raw-stewie --prefix actions/day_interacted_at=2018-08-28/ --dataset stewie --table actions_raw
'''
CHECKPOINT_FOLDER_PREFIX = '_gsbq_metadata/'
TEMP_PATH = '/tmp/processed_blobs.txt'
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO, stream=sys.stdout)


def run_import(bucket, prefix, dataset, table):
    all_blobs = list_blobs_with_prefix(bucket, prefix)
    logging.info('Found {} blobs in given path'.format(len(all_blobs)))
    processed_blobs = get_processed_blob_names(bucket, prefix)
    logging.info('Found {} already processed blobs in given path'.format(len(processed_blobs)))
    non_processed_blobs = get_non_processed_blobs(all_blobs, processed_blobs)
    if len(non_processed_blobs) > 0:
        logging.info('Sending {} blobs to BigQuery'.format(len(non_processed_blobs)))
        uploaded = send_to_bigquery(bucket, non_processed_blobs, dataset, table)
        logging.info('Uploaded {} blobs to BigQuery'.format(len(uploaded)))
        update_processed_blobs(uploaded)
        upload_blob(bucket, TEMP_PATH, get_checkpoint_file_name(prefix))
    logging.info('Deleting temp file')
    os.remove(TEMP_PATH)
    logging.info('Arigatcho, agilizou!')


def build_gs_uri(bucket, blob):
    return 'gs://{}/{}'.format(bucket, blob)


def send_to_bigquery(bucket, blobs, dataset, table):
    uris = [build_gs_uri(bucket, blob) for blob in blobs]
    try:
        result = load_tables_from_uri(uris, dataset, table)
        if result.error_result:
            raise Exception(result.error_result)
    except Exception as e:
        logging.error('failed to send blobs to big query: {}'.format(e))
        return []
    return blobs


def update_processed_blobs(uploaded):
    with open(TEMP_PATH, mode='a') as f:
        for success in uploaded:
            f.write(success + '\n')


def list_blobs_with_prefix(bucket_name, prefix):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    return [blob.name for blob in bucket.list_blobs(prefix=prefix) if blob.name != prefix]


def build_checkpoint_file_name(target_folder):
    return CHECKPOINT_FOLDER_PREFIX + 'checkpoint_' + target_folder + '.txt'


def get_checkpoint_file_name(prefix):
    root_folder, target_folder, _ = prefix.split('/')
    return root_folder + '/' + build_checkpoint_file_name(target_folder)


def get_processed_blob_names(bucket, prefix):
    download_blob(bucket, get_checkpoint_file_name(prefix), TEMP_PATH)
    return parse_processed_blob_names(TEMP_PATH)


def parse_processed_blob_names(path):
    try:
        with open(path) as f:
            processed_blobs = f.read().split('\n')
            return [blob for blob in processed_blobs if len(blob) > 0]
    except OSError:
        logging.warn('Did not find checkpoint file in temp location. If this is the first execution for given parameters, the file will be created automatically')
        return []


def download_blob(bucket_name, source_blob_name, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    try:
        blob.download_to_filename(destination_file_name)
    except (NotFound, OSError):
        logging.warn('Did not find a checkpoint file for given prefix and bucket. If this is the first execution for given parameters, the file will be created automatically')


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    logging.info('File {} uploaded to {}/{}'.format(
        source_file_name,
        bucket_name,
        destination_blob_name))


def load_tables_from_uri(blob_uri, dataset_id, table_id):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.PARQUET
    load_job = client.load_table_from_uri(
        blob_uri,
        dataset_ref.table(table_id),
        job_config=job_config)
    logging.info('Starting job {}'.format(load_job.job_id))
    return load_job.result()


def get_non_processed_blobs(blobs, processed_blobs):
    return [x for x in blobs if x not in processed_blobs]
