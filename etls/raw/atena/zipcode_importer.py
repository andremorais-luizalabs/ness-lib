# coding: utf-8
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("ZIPCODE_IMPORTER") \
    .enableHiveSupport() \
    .getOrCreate()


# Loads the Table
cep_df = spark.read.option("delimiter", "|") \
    .csv("gs://prd-lake-transient-atena/atena/cep_correio/")


# Renaming and transforming columns
cep = cep_df.select(
    col("_c0").alias("cep"),
    col("_c1").alias("uf_cep"),
    col("_c2").alias("cidade_cep"),
    col("_c3").alias("bairro_cep"),
    col("_c4").alias("codlograd_cep"),
    col("_c5").alias("lograd_cep"),
    col("_c6").alias("endereco_cep"),
    col("_c7").alias("de_cep"),
    col("_c8").alias("ate_cep"),
    col("_c9").alias("tipo_cep")
    ) \
    .withColumn("datalog", lit(datetime.date.today()))


# Stores in Parquet format
cep.write.mode("overwrite").parquet("gs://prd-lake-raw-atena/zipcode/")
