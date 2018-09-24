from .metadata import save_metadata, get_destination_path
from .utils.log import get_logger


def dataframe2raw(dataframe, namespace, dataset, file_format='parquet', partition_by=None):
    '''
    Save spark dataframe to raw zone
    :param dataframe: Spark Dataframe
    :param namespace: [str] Valid namespace
    :param dataset: [str] Valid dataset name
    :param file_format: [str] Valid format (parquet, csv, json)
    :param partition_by: [List] List of columns to partition by
    '''

    get_logger().info("Saving dataset: {} {} on Raw zone").format(namespace, dataset)
    _write_dataframe(dataframe, "raw", namespace, dataset, file_format, partition_by)


def dataframe2trusted(dataframe, namespace, dataset, file_format='parquet', partition_by=None):
    '''
        Save spark dataframe to trusted zone
        :param dataframe: Spark Dataframe
        :param namespace: [str] Valid namespace
        :param dataset: [str] Valid dataset name
        :param file_format: [str] Valid format (parquet, csv, json)
        :param partition_by: [List] List of columns to partition by
    '''
    get_logger().info("Saving dataset: {} {} on Trusted zone").format(namespace, dataset)
    _write_dataframe(dataframe, "trusted", namespace, dataset, file_format, partition_by)


def dataframe2refined(dataframe, namespace, dataset, file_format='parquet', partition_by=None):
    '''
        Save spark dataframe to refined zone
        :param dataframe: Spark Dataframe
        :param namespace: [str] Valid namespace
        :param dataset: [str] Valid dataset name
        :param file_format: [str] Valid format (parquet, csv, json)
        :param partition_by: [List] List of columns to partition by
        '''
    get_logger().info("Saving dataset: {} {} on Refined zone").format(namespace, dataset)
    _write_dataframe(dataframe, "refined", namespace, dataset, file_format, partition_by)


def _write_dataframe(dataframe,
                     zone,
                     namespace,
                     dataset,
                     file_format='parquet',
                     partition_by=None):
    """
    Writes agnostic dataframe to data lake
    """
    destination_path = "".join(get_destination_path(zone, namespace, dataset))
    if not partition_by:
        partition_by = []

    get_logger().info('destination: %s', destination_path)

    dataframe \
        .write \
        .mode('overwrite') \
        .partitionBy(partition_by) \
        .format(file_format)\
        .save(destination_path)

    save_metadata(dataframe,
                  zone,
                  namespace,
                  dataset,
                  file_format,
                  partition_by)
    get_logger().info("Write Dataset completed")
