from .metadata import save_metadata, get_destination_path
from .utils.log import get_logger


def dataframe2raw(dataframe, namespace, dataset, file_format='parquet', partition_by=None, mode='overwrite'):
    """
    Save spark dataframe to raw zone
    :param dataframe: Spark Dataframe
    :param namespace: [str] Valid namespace
    :param dataset: [str] Valid dataset name
    :param file_format: [str] Valid format (parquet, csv, json)
    :param partition_by: [List] List of columns to partition by
    :param mode: [str] Valid mode (overwrite, append, ignore)
    """

    _write_dataframe(dataframe, "raw", namespace, dataset, mode, file_format, partition_by)

def dataframe2trusted(dataframe, namespace, dataset, file_format='parquet', partition_by=None, mode='overwrite'):
    """
        Save spark dataframe to trusted zone
        :param dataframe: Spark Dataframe
        :param namespace: [str] Valid namespace
        :param dataset: [str] Valid dataset name
        :param file_format: [str] Valid format (parquet, csv, json)
        :param partition_by: [List] List of columns to partition by
        :param mode: [str] Valid mode (overwrite, append, ignore)
    """

    _write_dataframe(dataframe, "trusted", namespace, dataset, mode, file_format, partition_by)


def dataframe2refined(dataframe, namespace, dataset, file_format='parquet', partition_by=None):
    """
        Save spark dataframe to refined zone
        :param dataframe: Spark Dataframe
        :param namespace: [str] Valid namespace
        :param dataset: [str] Valid dataset name
        :param file_format: [str] Valid format (parquet, csv, json)
        :param partition_by: [List] List of columns to partition by
        :param mode: [str] Valid mode (overwrite, append, ignore)
    """

    _write_dataframe(dataframe, "refined", namespace, dataset, mode, file_format, partition_by)


def _write_dataframe(dataframe,
                     zone,
                     namespace,
                     dataset,
                     etl_mode='overwrite',
                     file_format='parquet',
                     partition_by=None):
    """
    Writes agnostic dataframe to data lake
    :param dataframe: Spark Dataframe
    :param zone: [str] Valid zone (transient, raw, trusted, refined)
    :param namespace: [str] Valid namespace
    :param dataset: [str] Valid dataset name
    :param etl_mode: [str] Valid mode (overwrite, append, ignore)
    :param file_format: [str] Valid format (parquet, csv, json)
    :param partition_by: [List] List of columns to partition by
    :return:
    """

    destination_path = "".join(get_destination_path(zone, namespace, dataset, etl_mode))
    if not partition_by:
        partition_by = []

    get_logger().info("Saving zone: {}\n namespace: {} \ndataset: {} \n, etl_mode: {}\n "
                      "file_format: {}".format(zone, namespace, dataset, etl_mode, file_format))
    get_logger().info('destination: %s', destination_path)

    dataframe \
        .write \
        .mode(etl_mode) \
        .partitionBy(partition_by) \
        .format(file_format)\
        .save(destination_path)

    save_metadata(dataframe, zone, namespace, dataset, etl_mode, file_format, partition_by)
    get_logger().info("Write Dataset completed")
