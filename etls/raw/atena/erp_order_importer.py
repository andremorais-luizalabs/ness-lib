# coding: utf-8
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("ERP_ORDER_IMPORTER") \
    .enableHiveSupport() \
    .getOrCreate()


# Loads the data using readStream
order_schema = spark.read.option("delimiter", "|") \
    .csv("gs://prd-lake-transient-atena/atena/gemco_pedido/").schema
order_df = spark.readStream.option("delimiter", "|").csv(
    "gs://prd-lake-transient-atena/atena/gemco_pedido/",
    schema=order_schema)

# Renaming and transforming columns
order = order_df.selectExpr(
    "_c0 as chaveped",
    "_c1 as origem",
    "_c2 as codfil",
    "_c3 as tipoped",
    "_c4 as numpedven",
    "_c5 as numpedprinc",
    "_c6 as codagrup",
    "_c7 as codcli",
    "_c8 as nomcli",
    "_c9 as cpfcli",
    "_c10 as dtnasccli",
    "_c11 as codclipres",
    "date_format(_c12, 'yyyy-MM-dd') as dtpedido",
    "_c13 as hrpedido",
    "_c14 as dtentrega",
    "_c15 as cartao",
    "_c16 as slip",
    "_c17 as serie",
    "_c18 as codcanal",
    "_c19 as descrcanal",
    "_c20 as codvendr",
    "_c21 as cpfvendr",
    "_c22 as cdicontratado",
    "_c23 as tpped",
    "_c24 as filorig",
    "_c25 as condpgto",
    "_c26 as dscondpgto",
    "_c27 as numcxa",
    "_c28 as vlentrada",
    "_c29 as vlmercad",
    "_c30 as status",
    "_c31 as descrstatus",
    "_c32 as vljurosfin",
    "_c33 as tpnota",
    "_c34 as vltotal",
    "_c35 as vlrebate",
    "_c36 as flretira",
    "_c37 as qtdparcela",
    "_c38 as dt_primeira_prc",
    "_c39 as vlparcela",
    "_c40 as codfilent",
    "_c41 as qtdparcelacdc",
    "_c42 as vlrparcelacdc",
    "_c43 as codfinanc",
    "_c44 as financeira",
    "_c45 as descfinanceira",
    "_c46 as codformapagtoprinc",
    "_c47 as formapagtoprinc",
    "_c48 as datalog")

# Stores in Parquet format
squery = order.writeStream.partitionBy("dtpedido") \
    .format("parquet") \
    .trigger(once=True).option(
        "checkpointLocation",
        "gs://prd-lake-transient-atena/checkpoints/gemco_pedido/") \
    .option("path", "gs://prd-lake-raw-atena/erp_order/") \
    .start() \
    .awaitTermination()


# Loads the Order Details files using readStream
detail_schema = spark.read.option("delimiter", "|") \
    .csv("gs://prd-lake-transient-atena/atena/gemco_pedido_item/").schema
detail_df = spark.readStream.option("delimiter", "|").csv(
    "gs://prd-lake-transient-atena/atena/gemco_pedido_item/",
    schema=detail_schema)

# Renaming and transforming columns
detail = detail_df.selectExpr(
    "_c0 as chavepeditem",
    "_c1 as chaveped",
    "_c2 as origem",
    "_c3 as codfil",
    "_c4 as tipoped",
    "_c5 as numpedven",
    "_c6 as coditprod",
    "_c7 as item",
    "_c8 as codprodf",
    "_c9 as qtdvenda",
    "_c10 as vlprecounit",
    "_c11 as vlprecounitliq",
    "_c12 as vlprecotab",
    "_c13 as vltotitem",
    "_c14 as vlrebate",
    "_c15 as status",
    "_c16 as flkit",
    "_c17 as codtpserv",
    "_c18 as descrcodtpserv",
    "_c19 as tpservico",
    "_c20 as numerocontrato",
    "_c21 as cttecnico",
    "_c22 as nrserie",
    "_c23 as datalog") \
    .withColumn("etl_id", lit(datetime.date.today()))

# Stores in Parquet format
squery = detail.writeStream.partitionBy("etl_id") \
    .format("parquet") \
    .trigger(once=True).option(
        "checkpointLocation",
        "gs://prd-lake-transient-atena/checkpoints/gemco_pedido_item/") \
    .option("path", "gs://prd-lake-raw-atena/erp_order_detail/") \
    .start() \
    .awaitTermination()
