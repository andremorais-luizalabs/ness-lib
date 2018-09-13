# coding: utf-8
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("EMPLOYEE_BRANCH_IMPORTER") \
    .enableHiveSupport() \
    .getOrCreate()


# Getting file for process
today = datetime.date.today().strftime("%Y%m%d")


# Source of the employees
employee_source_path = \
    "gs://prd-lake-transient-atena/atena/" \
    "funcionario/crm_funcionario_{}.txt.gz" \
    .format(today)

# Loading employees
foundEmployee = True
try:
    # Imports the files
    func_df = spark.read.option("delimiter", "|") \
        .csv(employee_source_path)
except:
    foundEmployee = False

# If exists, processing employees
if foundEmployee:

    # Renaming and transforming columns
    func = func_df.select(
        col("_c0").alias("cpf"),
        col("_c1").alias("codfil"),
        col("_c2").alias("chapa"),
        col("_c3").alias("cdicontratado"),
        col("_c4").alias("filial_atuacao"),
        col("_c5").alias("nome"),
        col("_c6").alias("codfuncao"),
        col("_c7").alias("funcao"),
        col("_c8").alias("situacao_codigo"),
        col("_c9").alias("situacao_data"),
        col("_c10").alias("status"),
        col("_c11").alias("id_user")
        ) \
        .withColumn("datalog", lit(datetime.date.today()))

    # Stores in Parquet format
    func.write.mode("overwrite") \
        .parquet("gs://prd-lake-raw-atena/employee/")


# Source of the branchs
branch_source_path = \
    "gs://prd-lake-transient-atena/atena/" \
    "gemco_filial/crm_gemco_filial_{}.txt.gz" \
    .format(today)

# Loading branchs
foundBranch = True
try:
    filial_df = spark.read.option("delimiter", "|") \
        .csv(branch_source_path)
except:
    foundBranch = False

# If exists, processing branchs
if foundBranch:

    # Renaming and transforming columns
    filial = filial_df.select(
        col("_c0").alias("codfil"),
        col("_c1").alias("dsfilial"),
        col("_c2").alias("tpfilial"),
        col("_c3").alias("desctpfilial"),
        col("_c4").alias("fantasia"),
        col("_c5").alias("razao_social"),
        col("_c6").alias("cgccpf"),
        col("_c7").alias("inscricao"),
        col("_c8").alias("codregiao"),
        col("_c9").alias("descregiao"),
        col("_c10").alias("endereco"),
        col("_c11").alias("numero"),
        col("_c12").alias("bairro"),
        col("_c13").alias("cidade"),
        col("_c14").alias("estado"),
        col("_c15").alias("cep"),
        col("_c16").alias("ddd"),
        col("_c17").alias("fone"),
        col("_c18").alias("emailloja"),
        col("_c19").alias("emailapoio"),
        col("_c20").alias("codfildist"),
        col("_c21").alias("fantasiatlv"),
        col("_c22").alias("status"),
        col("_c23").alias("coddivisional"),
        col("_c24").alias("descdivisional")) \
        .withColumn("datalog", lit(datetime.date.today()))

    # Stores in Parquet format
    filial.write.mode("overwrite") \
        .parquet("gs://prd-lake-raw-atena/erp_branch/")
