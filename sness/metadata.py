from datetime import datetime
from .config.config import BUCKETS
from .config.metadata import GET_METADATA, INSERT_METADATA

def save_metadata(df, zone, namespace, dataset, partition_columns=None):
    columns = df.columns
    last_update = datetime.now()
    file_format = "PARQUET"
    destination_bucket = BUCKETS.get(zone).get(namespace)
    destination_path = "{}/{}".format(dataset, last_update.strftime("%Y%m%d%H%M%S"))
    query = INSERT_METADATA.format(zone=zone, namespace=namespace, dataset=dataset,
                                   columns=columns, last_update=last_update,
                                   file_format=file_format, bucket=destination_bucket,
                                   source_path='', destination_path=destination_path,
                                   partition_columns=partition_columns)

    return query