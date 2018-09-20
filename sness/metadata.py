from datetime import datetime
from .config.config import BUCKETS
from .config.matedata import GET_METADATA, INSERT_METADATA

def save_metadata(df, zone, namespace, dataset, partition_column=None):
    columns = df.columns
    last_update = datetime.now()
    file_format = "PARQUET"
    destination_bucket = BUCKETS.get(zone).get(namespace)
    destination_path = "/".join(dataset, last_update.strftime("%Y%m%d%H%M%S"))
    query = INSERT_METADATA.format(zone=zone, namespace=namespace, dataset=dataset,
                                   columns=columns, last_update=last_update,
                                   file_format=file_format, bucket=destination_bucket,
                                   source_path='', destination_path=destination_path,
                                   partition_column=partition_column)

    return query