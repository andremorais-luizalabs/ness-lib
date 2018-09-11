import argparse
import os
import time
import googleapiclient.discovery
from google.cloud import storage
from ..config import config

zone = config.ZONE
region = config.REGION
project = config.PROJECT


# [START get_client]
def get_client():
    """Builds an http client authenticated with the service account
    credentials."""
    dataproc = googleapiclient.discovery.build('dataproc', 'v1')
    return dataproc

# [END get_client]


def get_default_pyspark_file():
    """Gets the PySpark file from this directory"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    f = open(os.path.join(current_dir), 'rb')
    return f


def get_pyspark_file(filename):
    f = open(filename, 'rb')
    return f, os.path.basename(filename)


def upload_pyspark_file(project_id, bucket_name, filename, file):
    """Uploads the PySpark file in this directory to the configured
    input bucket."""
    print('Uploading pyspark file to GCS')
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_file(file)


def download_output(project_id, cluster_id, output_bucket, job_id):
    """Downloads the output file from Cloud Storage and returns it as a
    string."""
    print('Downloading output file')
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(output_bucket)
    output_blob = (
        'google-cloud-dataproc-metainfo/{}/jobs/{}/driveroutput.000000000'
            .format(cluster_id, job_id))
    return bucket.blob(output_blob).download_as_string()


# [START create_cluster]
def create_cluster(client, project, zone, region, cluster_name, number_nodes=10, cpu=16):
    """
    Create a dataproc cluster on SuperNess platform
    :param client: dataproc client
    :param project:
    :param zone:
    :param region:
    :param cluster_name:
    :param number_nodes:
    :param cpu:
    :return:
    """
    print('Creating cluster...')
    zone_uri = \
        'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'.format(
            project, zone)
    cluster_data = {
        'projectId': project,
        'clusterName': cluster_name,
        'config': {
            'gceClusterConfig': {
                'zoneUri': zone_uri,
                'metadata': {
                    "JUPYTER_PORT": "8124",
                    "JUPYTER_CONDA_PACKAGES": "numpy:pandas:scikit-learn"
                }
            },
            "configBucket": "prd-cluster-exploration",
            'masterConfig': {
                'numInstances': 1,
                'machineTypeUri': 'n1-standard-{}'.format(str(cpu))
            },
            'workerConfig': {
                'numInstances': number_nodes,
                'machineTypeUri': 'n1-standard-{}'.format(str(cpu))
            },
            "softwareConfig": {
                "properties": {
                    "spark:spark.debug.maxToStringFields": "250",
                    "spark:spark.default.parallelism": "290",
                    "spark:spark.driver.cores": "5",
                    "spark:spark.driver.memory": "17G",
                    "spark:spark.executor.cores": "5",
                    "spark:spark.executor.instances": "29",
                    "spark:spark.executor.memory": "17G",
                    "spark:spark.executorEnv.PYTHONHASHSEED": "0",
                    "spark:spark.yarn.am.memory": "2048m",
                    "spark:spark.yarn.driver.memoryOverhead": "2048",
                    "spark:spark.yarn.executor.memoryOverhead": "2048"
                }
            },
            "initializationActions": {
                "executableFile": "gs://prd-cluster-config/dataproc/jupyter.sh",
                "executionTimeout": "600s"
            }
        }
    }

    result = client.projects().regions().clusters().create(
        projectId=project,
        region=region,
        body=cluster_data).execute()
    return result


# [END create_cluster]


def wait_for_cluster_creation(dataproc, project_id, region, cluster_name):
    print('Waiting for cluster creation...')

    while True:
        result = dataproc.projects().regions().clusters().list(
            projectId=project_id,
            region=region).execute()
        cluster_list = result['clusters']
        cluster = [c
                   for c in cluster_list
                   if c['clusterName'] == cluster_name][0]
        if cluster['status']['state'] == 'ERROR':
            raise Exception(result['status']['details'])
        if cluster['status']['state'] == 'RUNNING':
            print("Cluster created.")
            break


# [START list_clusters_with_detail]
def list_clusters_with_details(dataproc, project, region):
    result = dataproc.projects().regions().clusters().list(
        projectId=project,
        region=region).execute()
    try:
        cluster_list = result['clusters']
        for cluster in cluster_list:
            print("{} - {}"
                  .format(cluster['clusterName'], cluster['status']['state']))
        return result
    except KeyError:
        return None


# [END list_clusters_with_detail]


def list_clusters(dataproc, project, region):
    result = list_clusters_with_details(dataproc, project, region)
    try:
        cluster_list = result['clusters']
        lista = dict()
        for cluster in cluster_list:
            lista.update({cluster['clusterName']: cluster['status']['state']})
        return lista
    except KeyError:
        return list()


def get_cluster_id_by_name(cluster_list, cluster_name):
    """Helper function to retrieve the ID and output bucket of a cluster by
    name."""
    cluster = [c for c in cluster_list if c['clusterName'] == cluster_name][0]
    return cluster['clusterUuid'], cluster['config']['configBucket']


# [START submit_pyspark_job]
def submit_pyspark_job(dataproc, project, region,
                       cluster_name, bucket_name, filename, monitor=None):
    """Submits the Pyspark job to the cluster, assuming `filename` has
    already been uploaded to `bucket_name`"""
    job_details = {
        'projectId': project,
        'job': {
            'placement': {
                'clusterName': cluster_name
            },
            'pysparkJob': {
                'mainPythonFileUri': 'gs://{}/{}'.format(bucket_name, filename)
            }
        }
    }
    result = dataproc.projects().regions().jobs().submit(
        projectId=project,
        region=region,
        body=job_details).execute()
    job_id = result['reference']['jobId']
    print('Submitted job ID {}'.format(job_id))
    if monitor:
        return wait_for_job(dataproc, project, region, job_id)
    return job_id


# [END submit_pyspark_job]


# [START delete]
def delete_cluster(dataproc, project, region, cluster):
    print('Tearing down cluster')
    result = dataproc.projects().regions().clusters().delete(
        projectId=project,
        region=region,
        clusterName=cluster).execute()
    return result


# [END delete]


# [START wait]
def wait_for_job(dataproc, project, region, job_id):
    print('Waiting for job to finish...')
    while True:
        result = dataproc.projects().regions().jobs().get(
            projectId=project,
            region=region,
            jobId=job_id).execute()
        # Handle exceptions
        if result['status']['state'] == 'ERROR':
            raise Exception(result['status']['details'])
        elif result['status']['state'] == 'DONE':
            print('Job finished.')
            return result
        time.sleep(10)


# [END wait]


def main(project_id, zone, cluster_name, bucket_name,
         pyspark_file=None, create_new_cluster=True):
    dataproc = get_client()
    try:
        if pyspark_file:
            spark_file, spark_filename = get_pyspark_file(pyspark_file)

        if create_new_cluster:
            create_cluster(
                dataproc, project_id, zone, region, cluster_name)
            wait_for_cluster_creation(
                dataproc, project_id, region, cluster_name)

        upload_pyspark_file(
            project_id, bucket_name, spark_filename, spark_file)

        cluster_list = list_clusters_with_details(
            dataproc, project_id, region)['clusters']

        (cluster_id, output_bucket) = (
            get_cluster_id_by_name(cluster_list, cluster_name))

        # [START call_submit_pyspark_job]
        job_id = submit_pyspark_job(
            dataproc, project_id, region,
            cluster_name, bucket_name, spark_filename)
        # [END call_submit_pyspark_job]
        wait_for_job(dataproc, project_id, region, job_id)

        output = download_output(project_id, cluster_id, output_bucket, job_id)
        print('Received job output {}'.format(output))
        return output
    finally:
        if create_new_cluster:
            delete_cluster(dataproc, project_id, region, cluster_name)
        spark_file.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--project_id', help='Project ID you want to access.', required=True),
    parser.add_argument(
        '--zone', help='Zone to create clusters in/connect to', required=True)
    parser.add_argument(
        '--cluster_name',
        help='Name of the cluster to create/connect to', required=True)
    parser.add_argument(
        '--gcs_bucket', help='Bucket to upload Pyspark file to', required=True)
    parser.add_argument(
        '--pyspark_file', help='Pyspark filename. Defaults to pyspark_sort.py')
    parser.add_argument(
        '--create_new_cluster',
        action='store_true', help='States if the cluster should be created')

    args = parser.parse_args()
    main(
        args.project_id, args.zone, args.cluster_name,
        args.gcs_bucket, args.pyspark_file, args.create_new_cluster)
