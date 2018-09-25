from datetime import datetime
from .config.config import BUCKETS, FOO, HOST
from .config.metadata import GET_METADATA, INSERT_METADATA
from .utils.postgresql import connect, run_query
from .utils.log import get_logger
import json


def save_metadata(dataframe, zone, namespace, dataset, etl_mode, file_format="parquet", partition_columns=None):
    """
    This method saves the dataset metadata on postgresql
    """

    columns = json.dumps(dict(dataframe.dtypes))
    last_update = datetime.now()
    destination_bucket, destination_path = get_destination_path(zone, namespace, dataset, etl_mode)
    partition_columns = ','.join(partition_columns) if partition_columns else partition_columns
    query = INSERT_METADATA.format(zone=zone, namespace=namespace, dataset=dataset,
                                   columns=columns, last_update=last_update,
                                   file_format=file_format, bucket=destination_bucket,
                                   source_path='', destination_path=destination_path,
                                   partition_columns=partition_columns)
    conn = connect(HOST, "sness-catalog", "postgres", FOO)
    run_query(conn, query)


def get_destination_path(zone, namespace, dataset, etl_mode):
    """
    This method calculates where to save the dataset based on etl_mode
    """

    now = datetime.now()
    destination_path = "{}/{}".format(dataset, now.strftime("%Y%m%d%H%M%S"))
    if etl_mode in ("append", "ignore"):
        destination_path = dataset + "/"
    try:
        destination_bucket = 'gs://' + BUCKETS.get(zone).get(namespace) + '/'
    except AttributeError as e:
        get_logger().info("Unknow zone or namespace, please visit config to check")

    return destination_bucket, destination_path


def get_metadata():
    query = GET_METADATA
    conn = connect(HOST, "sness-catalog", "postgres", FOO)
    cur = run_query(conn, query)
    return cur.fetchall()
