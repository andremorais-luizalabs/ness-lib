# coding: utf-8
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("GC_CUSTOMER_IMPORTER") \
    .enableHiveSupport() \
    .getOrCreate()


# Getting current date for process
today = datetime.date.today().strftime("%Y%m%d")


# Source of the GC Customers products/
gc_source_path = \
    "gs://prd-lake-transient-atena/atena/gc_cliente/crm_gc_cliente_{}.txt.gz" \
    .format(today)

# Loading products
foundGC = True
try:
    gc_df = spark.read.option("delimiter", "|") \
        .csv(gc_source_path)
except:
    foundGC = False

# If exists, processing products
if foundGC:

    # Renaming and transforming columns
    gc = gc_df.select(
        col("_c0").alias("numdbm"),
        col("_c1").alias("codcli"),
        col("_c2").alias("cpf"),
        col("_c3").alias("nome"),
        col("_c4").alias("clusterecommid"),
        col("_c5").alias("sexo"),
        col("_c6").alias("datanascimento"),
        col("_c7").alias("dtcadastro"),
        col("_c8").alias("dt_cadastro_ecommerce"),
        col("_c9").alias("fl_cadecommerce"),
        col("_c10").alias("fl_maia"),
        col("_c11").alias("fl_funcionarioid"),
        col("_c12").alias("fl_clienteouro"),
        col("_c13").alias("fl_cartaoml"),
        col("_c14").alias("email"),
        col("_c15").alias("emailecomm"),
        col("_c16").alias("optemail"),
        col("_c17").alias("celulartratado"),
        col("_c18").alias("telefonetratado"),
        col("_c19").alias("optsms"),
        col("_c20").alias("estado"),
        col("_c21").alias("cidadetratada"),
        col("_c22").alias("bairro"),
        col("_c23").alias("cep"),
        col("_c24").alias("cartaoml"),
        col("_c25").alias("filialcadastroid"),
        col("_c26").alias("filialcompraid"),
        col("_c27").alias("cartao"),
        col("_c28").alias("perfilcompra"),
        col("_c29").alias("fl_ativo"),
        col("_c30").alias("dtultimacompra"),
        col("_c31").alias("compra"),
        col("_c32").alias("recencia"),
        col("_c33").alias("qtd_compra"),
        col("_c34").alias("vlr_compra"),
        col("_c35").alias("modelo_quem"),
        col("_c36").alias("fl_clienteouro20"),
        col("_c37").alias("fl_comprouecom_2017"),
        col("_c38").alias("fl_comprouecom_2018"),
        col("_c39").alias("clv"),
        col("_c40").alias("segmento_clv")) \
        .withColumn("datalog", lit(datetime.date.today()))

    # Stores in Parquet format
    gc.write.mode("overwrite").parquet("gs://prd-lake-raw-atena/gc_customer/")


# Imports the Table
ind_df = spark.read.option("delimiter", "|") \
    .csv("gs://prd-lake-transient-atena/atena/gc_cliente_indice/")

# Renaming and transforming columns
ind = ind_df.select(
    col("_c0").alias("idorigem"),
    col("_c1").alias("origem"),
    col("_c2").alias("numdbm")) \
    .withColumn("datalog", lit(datetime.date.today()))

# Stores in Parquet format
ind.write.mode("overwrite") \
    .parquet("gs://prd-lake-raw-atena/gc_customer_index/")
