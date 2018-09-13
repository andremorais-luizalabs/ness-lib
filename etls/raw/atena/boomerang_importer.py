# coding: utf-8
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("BOOMERANG_IMPORTER") \
    .enableHiveSupport() \
    .getOrCreate()


# Getting current date for process
today = datetime.date.today().strftime("%Y%m%d")

# Source of the bommerang list types
list_type_source_path = \
    "gs://prd-lake-transient-atena/atena/" \
    "bumer_tipo_lista/crm_bumer_tipo_lista_{}.txt.gz" \
    .format(today)

# Loading list types
foundListType = True
try:
    list_type_df = spark.read.option("delimiter", "|") \
        .csv(list_type_source_path)
except:
    foundListType = False

# If exists, processing list types
if foundListType:

    # Renaming and transforming columns
    list_type = list_type_df.selectExpr(
        "_c0 as intidtipolista",
        "_c1 as strtitulolista",
        "_c2 as strdescrlista",
        "_c3 as intpontos",
        "_c4 as dtaalteracao",
        "_c5 as dtainicio",
        "_c6 as dtatermino",
        "_c7 as intidgrupo",
        "_c8 as intidsubgrupo",
        "_c9 as dtainclusao",
        "_c10 as usuario",
        "_c11 as chrativo",
        "_c12 as intperiodoretorno") \
        .withColumn("datalog", lit(datetime.date.today()))

    # Stores in Parquet format
    list_type.write.mode("overwrite") \
        .parquet("gs://prd-lake-raw-atena/boomer_list_type/")


# Loads the contacts using readStream
contact_schema = spark.read.option("delimiter", "|") \
    .csv("gs://prd-lake-transient-atena/atena/bumer_contato/").schema
contact_df = spark.readStream.option("delimiter", "|") \
    .csv("gs://prd-lake-transient-atena/atena/bumer_contato/", schema=contact_schema)

# Renaming and transforming columns
contact = contact_df.selectExpr(
    "_c0 as intidlista",
    "_c1 as cartao",
    "_c2 as strnome",
    "_c3 as telefone",
    "_c4 as celular",
    "_c5 as intidtipolista",
    "_c6 as intidcontato",
    "_c7 as strcontato",
    "date_format(_c8, 'yyyy-MM-dd') as dtacontato",
    "_c8 as dthrcontato",
    "_c9 as chrgerouponto",
    "_c10 as intidpontuacao",
    "_c11 as cdicontratado",
    "_c12 as dtaalteracao",
    "_c13 as dtaabertura",
    "_c14 as dtafechamento") \
    .withColumn("datalog", lit(datetime.date.today()))

# Stores in Parquet format
squery = contact.writeStream.partitionBy("dtacontato") \
    .format("parquet") \
    .trigger(once=True) \
    .option("checkpointLocation", "gs://prd-lake-transient-atena/checkpoints/bumer_contato/") \
    .option("path", "gs://prd-lake-raw-atena/boomer_contact/") \
    .start() \
    .awaitTermination()


# Loads the customer list type using readStream
cust_list_type_schema = spark.read.option("delimiter", "|") \
    .csv("gs://prd-lake-transient-atena/atena/bumer_cli_tipo_lista/").schema
cust_list_type_df = spark.readStream.option("delimiter", "|") \
    .csv("gs://prd-lake-transient-atena/atena/bumer_cli_tipo_lista/", schema=cust_list_type_schema)

# Renaming and transforming columns
cust_list_type = cust_list_type_df.selectExpr(
    "_c0 as intcartao",
    "_c1 as intidtipolista",
    "_c2 as intfilial",
    "_c3 as intdisponivel",
    "_c4 as dtaalteracao",
    "_c5 as intidgrupo",
    "_c6 as intidsubgrupo",
    "_c7 as strofertapersonalizada",
    "_c8 as dtaimportacao") \
    .withColumn("datalog", lit(datetime.date.today()))

# Stores in Parquet format
squery = cust_list_type.writeStream.partitionBy("intidtipolista") \
    .format("parquet") \
    .trigger(once=True) \
    .option("checkpointLocation", "gs://prd-lake-transient-atena/checkpoints/bumer_cli_tipo_lista/") \
    .option("path", "gs://prd-lake-raw-atena/boomer_cust_list_type/") \
    .start() \
    .awaitTermination()
