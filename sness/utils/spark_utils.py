"""
Spark Utils
"""

from pyspark.sql import SparkSession
from .log import get_logger


def get_spark_session():
    """
    Get Spark Session
    """
    try:
        return SparkSession.builder.getOrCreate()
    except OSError as exc:
        get_logger().error('Spark Session Not Found!')
        get_logger().exception(exc)
        raise Exception('Invalid Spark Session')


def get_spark_context():
    """
    Get Spark Context
    """
    return get_spark_session()._sc
