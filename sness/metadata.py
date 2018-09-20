from datetime import datetime
from .config.config import BUCKETS, FOO, HOST
from .config.metadata import GET_METADATA, INSERT_METADATA
from .utils.postgresql import connect, run_query
import json

def save_metadata(df, zone, namespace, dataset, partition_columns=None):
    columns = json.dumps(dict(df.dtypes))
    last_update = datetime.now()
    file_format = "PARQUET"
    destination_bucket = BUCKETS.get(zone).get(namespace)
    destination_path = "{}/{}".format(dataset, last_update.strftime("%Y%m%d%H%M%S"))
    partition_columns = ','.join(partition_columns) if partition_columns else partition_columns
    query = INSERT_METADATA.format(zone=zone, namespace=namespace, dataset=dataset,
                                   columns=columns, last_update=last_update,
                                   file_format=file_format, bucket=destination_bucket,
                                   source_path='', destination_path=destination_path,
                                   partition_columns=partition_columns)
    conn = connect(HOST, "sness-catalog", "postgres", FOO)
    run_query(conn, query)


def get_metadata():
    query = GET_METADATA
    conn = connect(HOST, "sness-catalog", "postgres", FOO)
    cur = run_query(conn, query)
    return cur.fetchall()
