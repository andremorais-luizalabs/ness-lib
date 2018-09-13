# coding: utf-8
import datetime
import calendar
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Starts the Spark Session
spark = SparkSession \
    .builder \
    .appName("CONSOLIDATION_CUSTOMER_SALES") \
    .enableHiveSupport() \
    .getOrCreate()


# Create a SQL Context
sc = spark.sparkContext
sqlContext = SQLContext(sc)

# Reading parquet file
df = sqlContext.read.parquet("gs://prd-lake-raw-atena/erp_order/")

# Creates a temporary view using the DataFrame
df.createOrReplaceTempView("order")


# Date of closed jurisdiction
now = datetime.datetime.now().date()
comp = now.replace(day=1).replace(month=now.month-1)
dtcompetencia = comp.strftime("%Y-%m-%d %H:%M:%S")


# Date of last day of the month
lastday = calendar.monthrange(comp.year, comp.month)[1]
dt_ult_dia_mes = comp.replace(day=lastday)
dt_ult_dia_mes = dt_ult_dia_mes.strftime("%Y-%m-%d %H:%M:%S")

# Consolidation of customer informations in the MONTH
consolid_month_sale = spark.sql(
"select t.id_gemco "
      ",t.qtde_compra_mes "
      ",t.vl_total_compra_mes "
  "from (select codcli as id_gemco "
              ",count(distinct codfil, dtpedido) AS qtde_compra_mes "
              ",sum(vltotal) AS vl_total_compra_mes "
          "from order o "
         "where o.dtpedido between '%s' and '%s' "
         "group by codcli) t "
 "where 1=1" % (dtcompetencia, dt_ult_dia_mes))

# Creates a temporary view using the DataFrame
consolid_month_sale.createOrReplaceTempView("consolid_month_sale")


# Geting the date of 12 months ago counting the month of closed jurisdiction
dt_12_meses_atras = comp - datetime.timedelta(days=11*365/12)
dt_12_meses_atras = dt_12_meses_atras.strftime("%Y-%m-%d %H:%M:%S")

# Consolidation of customer informations in TWELVE MONTHS
consolid_12months_sale = spark.sql(
"select t.id_gemco "
       ",t.qtde_compra_12_meses "
       ",t.vl_total_compra_12_meses "
   "from (select codcli as id_gemco "
               ",count(distinct codfil, dtpedido) AS qtde_compra_12_meses "
               ",sum(vltotal) AS vl_total_compra_12_meses "
           "from order o "
          "where o.dtpedido between '%s' and '%s' "
          "group by codcli) t "
  "where 1=1" % (dt_12_meses_atras, dt_ult_dia_mes))

# Creates a temporary view using the DataFrame
consolid_12months_sale.createOrReplaceTempView("consolid_12months_sale")


# Consolidation of customer informations in LIFE
consolid_general_sale = spark.sql(
"select p.codcli as id_gemco "
      ",u.dt_primeira_compra "
      ",u.dt_ultima_compra "
      ",u.qtde_compra "
      ",u.vl_total_compra "
      ",p.qtdparcela as uc_Parcelas "
      ",p.formapagtoprinc as uc_meio_de_pagamento "
      ",case "
         "when u.reg_loja <> 0 and u.reg_site <> 0 then 'multicanal' "
         "when u.reg_loja = 0  and u.reg_site <> 0 then 'site' "
         "when u.reg_loja <> 0 and u.reg_site =  0 then 'loja' "
         "else 'nao identificado' end AS Perfil_Compra "
      ",u.flretiraloja "
  "from order p, "
      "(select o.codcli AS id_gemco "
             ",max(o.numpedven) AS ultimo_numpedven "
             ",min(dtpedido) AS dt_primeira_compra "
             ",max(dtpedido) AS dt_ultima_compra "
             ",count(distinct codfil, dtpedido) AS qtde_compra "
             ",sum(vltotal) AS vl_total_compra "
             ",sum(if(codfil = 200, 1, 0)) AS reg_site "
             ",sum(if(codfil <> 200 and codfil <> 660, 1, 0)) AS reg_loja "
             ",max(if(codfil = 200 and flretira = 'S', 1, 0)) AS flretiraloja "
        "from order o "
       "where o.dtpedido <= '%s' "
       "group by o.codcli) u "
 "where p.numpedven = u.ultimo_numpedven" % (dt_ult_dia_mes))

# Creates a temporary view using the DataFrame
consolid_general_sale.createOrReplaceTempView("consolid_general_sale")


# Register UDF for process
def dateformat(str_date):
    """
    Data format validator:
    str_date - Date to validate
    str_format - Current date format
    """
    if not str_date: return ''
    try:
        date = datetime.datetime.strptime(str_date, '%Y-%m-%d').date()
        strdate = date.strftime('%Y-%m-%d %H:%M:%S')
        return strdate
    except ValueError:
        return ''

sqlContext.udf.register("udf_dateformat", dateformat)


# Consolidation of the customer sales
consolid_customer_sales = spark.sql(
"select g.id_gemco "
      ",'%s' as dtcompetencia "
      ",udf_dateformat(g.dt_primeira_compra) as dt_primeira_compra "
      ",udf_dateformat(g.dt_ultima_compra) as dt_ultima_compra "
      ",g.qtde_compra "
      ",g.vl_total_compra "
      ",t.qtde_compra_12_meses "
      ",t.vl_total_compra_12_meses "
      ",m.qtde_compra_mes "
      ",m.vl_total_compra_mes "
      ",g.uc_Parcelas "
      ",g.uc_meio_de_pagamento "
      ",g.Perfil_Compra "
      ",g.flretiraloja "
  "from consolid_general_sale g "
       "LEFT OUTER JOIN "
       "consolid_12months_sale t "
           "on (g.id_gemco = t.id_gemco) "
       "LEFT OUTER JOIN "
       "consolid_month_sale m "
           "on (g.id_gemco = m.id_gemco)" % (dtcompetencia))

# Stores in Parquet format
consolid_customer_sales.write.mode("overwrite") \
    .parquet("gs://prd-lake-raw-atena/consolid_customer_sales/")
