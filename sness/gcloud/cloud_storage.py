import os
import time
import googleapiclient.discovery
from google.cloud import storage
from ..config import config
project_id = config.PROJECT

def get_client():
    """Builds an http client authenticated with the service account
    credentials."""
    return storage.Client(project=project_id)


def upload_file(bucket_name, filename, file):
    """Uploads the file in this directory to the configured
    input bucket."""
    client = get_client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_file(file)


def get_list_of_files(bucket_name, path):
    """
    Return a ordered list with the file names of a respective path.
    """
    client = get_client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    return blob