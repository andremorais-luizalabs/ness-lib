# coding: utf-8
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("SAC_IMPORTER") \
    .enableHiveSupport() \
    .getOrCreate()


# Loads the data using readStream
schema = spark.read.option("delimiter", "|") \
    .csv("gs://prd-lake-transient-atena/atena/sac_atendimento/").schema
sac_df = spark.readStream.option("delimiter", "|") \
    .csv("gs://prd-lake-transient-atena/atena/sac_atendimento/", schema=schema)

# Renaming and transforming columns
sac = sac_df.select(
    col("_c0").alias("origem"),
    col("_c1").alias("cod_atendimento"),
    col("_c2").alias("cpf_cnpj"),
    col("_c3").alias("tp_pessoa"),
    col("_c4").alias("dt_atendimento"),
    col("_c5").alias("nro_protocolo"),
    col("_c6").alias("cod_assunto"),
    col("_c7").alias("desc_assunto"),
    col("_c8").alias("cod_canal_atendimento"),
    col("_c9").alias("desc_canal_atendimento"),
    col("_c10").alias("tp_canal_venda")
    ) \
    .withColumn("datalog", lit(datetime.date.today()))

# Stores in Parquet format
squery = sac.writeStream.partitionBy("datalog") \
    .format("parquet") \
    .trigger(once=True).option(
        "checkpointLocation",
        "gs://prd-lake-transient-atena/checkpoints/sac_atendimento/") \
    .option("path", "gs://prd-lake-raw-atena/sac_calls/") \
    .start() \
    .awaitTermination()
