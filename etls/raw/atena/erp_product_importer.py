# coding: utf-8
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("ERP_PRODUCT_IMPORTER") \
    .enableHiveSupport() \
    .getOrCreate()

# Getting current date for process
today = datetime.date.today().strftime("%Y%m%d")


# Source of the employees products/
product_source_path = \
    "gs://prd-lake-transient-atena/atena/" \
    "gemco_produto/crm_gemco_produto_{}.txt.gz" \
    .format(today)

# Loading products
foundProducts = True
try:
    product_df = spark.read.option("delimiter", "|") \
        .csv(product_source_path)
except:
    foundProducts = False

# If exists, processing products
if foundProducts:

    # Renaming and transforming columns
    product = product_df.selectExpr(
        "_c0 as coditprod",
        "_c1 as digitprod",
        "_c2 as codprodf",
        "_c3 as descrprod",
        "_c4 as codlinha",
        "_c5 as descrlinha",
        "_c6 as codfam",
        "_c7 as descrfam",
        "_c8 as codgrupo",
        "_c9 as descrgrupo",
        "_c10 as codsubgp",
        "_c11 as descrsubgp",
        "_c12 as codcapac",
        "_c13 as descrcapac",
        "_c14 as codcor",
        "_c15 as descrcor",
        "_c16 as descrescor",
        "_c17 as especific",
        "_c18 as descrespecific",
        "_c19 as descresespecific",
        "_c20 as referencia",
        "_c21 as preco",
        "_c22 as codforne",
        "_c23 as fantasiaforne",
        "_c24 as cgccpfforne",
        "_c25 as dtatualizacao")

    # Stores in Parquet format
    product.write.mode("overwrite") \
        .parquet("gs://prd-lake-raw-atena/erp_product/")
