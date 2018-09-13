# coding: utf-8
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("SISCOB_IMPORTER") \
    .enableHiveSupport() \
    .getOrCreate()

# Getting current date for process
today = datetime.date.today().strftime("%Y%m%d")


# Importing siscob charges to raw
charges_source_path = \
    "gs://prd-lake-transient-atena/atena/" \
    "siscob_cobraveis/crm_siscob_cobraveis_{}.txt.gz" \
    .format(today)
try:
    # Imports the Order Table
    cobr_df = spark.read.option("delimiter", "|") \
        .csv(charges_source_path)

    # Renaming and transforming columns
    cobr = cobr_df.select(
        col("_c0").alias("origem"),
        col("_c1").alias("codagrup"),
        col("_c2").alias("codigo_cliente"),
        col("_c3").alias("data_inclusao"),
        col("_c4").alias("data_alteracao"),
        col("_c5").alias("qtd_cartas_aviso"),
        col("_c6").alias("negativado"),
        col("_c7").alias("data_vencimento"),
        col("_c8").alias("valor_divida"),
        col("_c9").alias("refinanciado"),
        col("_c10").alias("entrada_aberto"),
        col("_c11").alias("codigo_financeira"),
        col("_c12").alias("tipo_pessoa"),
        col("_c13").alias("tipo_cliente"),
        col("_c14").alias("proces_reab"),
        col("_c15").alias("divergente"),
        col("_c16").alias("valor_aberto_gemco")) \
        .withColumn("datalog", lit(datetime.date.today()))

    # Stores in Parquet format
    cobr.write.mode("overwrite") \
        .parquet("gs://prd-lake-raw-atena/siscob_charges/")
except:
    pass
