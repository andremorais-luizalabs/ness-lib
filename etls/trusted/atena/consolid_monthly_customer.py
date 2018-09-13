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
    .appName("CONSOLIDATION_MONTHLY_CUSTOMER") \
    .enableHiveSupport() \
    .getOrCreate()

# Create a SQL Context
sc = spark.sparkContext
sqlContext = SQLContext(sc)


# Creates a temporary view using the DataFrame
consolid_customer_df = sqlContext.read \
    .parquet("gs://prd-lake-raw-atena/consolid_customer_general/")
consolid_customer_df.createOrReplaceTempView("consolid_customer")


# Creates a temporary view using the DataFrame
purchase_branch_df = sqlContext.read \
    .parquet("gs://prd-lake-raw-atena/consolid_purchase_branch/")
purchase_branch_df.createOrReplaceTempView("purchase_branch")

# Creates a temporary view using the DataFrame
branch_df = sqlContext.read.parquet("gs://prd-lake-raw-atena/erp_branch/")
branch_df.createOrReplaceTempView("branch")

# Consolidation of the customer sales
purchase_branch_ds_df = spark.sql(
"select p.id_gemco "
      ",p.codfilcompra as filialcompraid "
      ",b.dsfilial as dsfilialcompra "
  "from purchase_branch p "
       "LEFT OUTER JOIN "
       "branch b "
           "ON (p.codfilcompra = b.codfil)")
purchase_branch_ds_df.createOrReplaceTempView("purchase_branch_ds")


# Creates a temporary view using the DataFrame
customer_sales_df = sqlContext.read \
    .parquet("gs://prd-lake-raw-atena/consolid_customer_sales/")
customer_sales_df.createOrReplaceTempView("customer_sales")


# Date of last day of the month (jurisdiction)
now = datetime.datetime.now().date()
comp = now.replace(day=1).replace(month=now.month-1)
lastday = calendar.monthrange(comp.year, comp.month)[1]
dt_ult_dia_mes = comp.replace(day=lastday).strftime("%Y-%m-%d %H:%M:%S")


# Declaration of the UDFs for the process
def active(qt_purch_year=0):
    """
    Checks if the customer is active.

    Parameters:
    - qt_purch_year: Quantity of purchase in the year
    """
    qt_purch_year = 0 if not qt_purch_year else qt_purch_year

    if qt_purch_year > 0:
        return 1
    else:
        return 0

sqlContext.udf.register("udf_active", active)


def recency(dt_last_purchase, end_dt_period):
    """
    Calculates the diference in months beween last date purchase and
    end date of the period.

    Parameters:
    - dt_last_purchase: Date of the last purchase
    - end_dt_period: End date of the period
    """
    if not dt_last_purchase or not end_dt_period:
        return None

    date1 = datetime.datetime.strptime(dt_last_purchase, '%Y-%m-%d %H:%M:%S')
    date2 = datetime.datetime.strptime(end_dt_period, '%Y-%m-%d %H:%M:%S')
    months = (date2.year - date1.year) * 12 + (date2.month - date1.month)
    return months

sqlContext.udf.register("udf_recency", recency, IntegerType())


def purchase_position(qt_purch_month=0, qt_purch_year=0, qt_purch_live=0, recency=0):
    """    Calculates the position of purchase of the customers.

    Parameters:
    - qt_purch_month: Quantity of purchase in the month
    - qt_purch_year: Quantity of purchase in 12 months
    - qt_purch_live: Purchase amount in life
    - recency: Diference in months beween last date purchase and end
               date of the period
    """

    # Preparation of parameters
    qt_purch_month = 0 if not qt_purch_month else qt_purch_month
    qt_purch_year = 0 if not qt_purch_year else qt_purch_year
    qt_purch_live = 0 if not qt_purch_live else qt_purch_live
    recency = 0 if not recency else recency

    # Eliminates intersections in quantities
    dif_purch_year = qt_purch_year - qt_purch_month
    dif_purch_live = qt_purch_live - qt_purch_year

    # Rule of the position of purchase
    if qt_purch_month > 0 and dif_purch_year == 0 and dif_purch_live == 0:
        return 'novo'
    elif qt_purch_month > 0 and dif_purch_year == 0 and dif_purch_live > 0:
        return 'reativado'
    elif qt_purch_month > 0 and dif_purch_year > 0:
        return 'ativo recompra'
    elif qt_purch_month == 0 and dif_purch_year > 0:
        return 'ativo sem compra'
    elif qt_purch_month == 0 and dif_purch_year == 0 and recency == 12:
        return 'churn'
    else:
        return 'inativo'

sqlContext.udf.register("udf_purchase_position", purchase_position)


def score(recency, qt_purch_year=0, vl_purch_year=0):
    """
    Captures the customer score

    Parameters:
    - recency: Diference in months beween last date purchase and end
               date of the period
    - qt_purch_year: Quantity of purchase the last 12 months
    - vl_purch_year: Monetary value of purchase in the last 12 months
    """

    # Preparation of parameters
    if recency != 0: recency = 99 if not recency else recency
    qt_purch_year = 0 if not qt_purch_year else qt_purch_year
    vl_purch_year = 0 if not vl_purch_year else vl_purch_year

    # Points by recency
    if   recency >=  0 and recency < 4:  p_recency = 5
    elif recency >=  4 and recency < 8:  p_recency = 4
    elif recency >=  8 and recency < 13: p_recency = 3
    elif recency >= 13 and recency < 18: p_recency = 2
    elif recency >= 18 and recency < 24: p_recency = 1
    else: p_recency = 0

    # Point by frequency
    if   qt_purch_year >= 5: p_frequency = 5
    elif qt_purch_year == 4: p_frequency = 4
    elif qt_purch_year == 3: p_frequency = 3
    elif qt_purch_year == 2: p_frequency = 2
    elif qt_purch_year == 1: p_frequency = 1
    else: p_frequency = 0

    # Point by value
    if   vl_purch_year >= 2000: p_monetary = 5
    elif vl_purch_year >= 1000 and vl_purch_year < 2000: p_monetary = 4
    elif vl_purch_year >=  500 and vl_purch_year < 1000: p_monetary = 3
    elif vl_purch_year >=  200 and vl_purch_year <  500: p_monetary = 2
    elif vl_purch_year >=    1 and vl_purch_year <  200: p_monetary = 1
    else: p_monetary = 0

    return (p_recency * 2.5) + (p_frequency * 1.5) + (p_monetary)

sqlContext.udf.register("udf_score", score)


def ltv(score):
    '''
    Captures the customer LTV classification.

    Parameters:
    - score: Customer score
    '''

    # Preparation of parameters
    score = 0 if not score else score

    # Rule of the LTV
    if   score > 21 and score <= 25: return 'G1'
    elif score > 17 and score <= 21: return 'G2'
    elif score > 13 and score <= 17: return 'G3'
    elif score >  9 and score <= 13: return 'G4'
    elif score >  5 and score <=  9: return 'G5'
    else: return 'NI'

sqlContext.udf.register("udf_ltv", ltv)


def add_months(sourcedate, months):

    if not sourcedate: return ''
    dateorig = datetime.datetime.strptime(sourcedate, '%Y-%m-%d %H:%M:%S')
    newdate = dateorig + datetime.timedelta(days=months*365/12)
    newdate = newdate.date().strftime("%Y-%m-%d %H:%M:%S")

    return newdate

sqlContext.udf.register("udf_add_months", add_months)


# Creating Single Consolidation
single_consolidation = spark.sql(
"select c.cpf_cnpj as id_single "
      ",s.dtcompetencia "
      ",c.flclienteouro "
      ",c.flfuncionario "
      ",0 as flcartaoml " # BUSCAR INFORMAÇÃO POSTERIORMENTE
      ",udf_active(s.qtde_compra_12_meses) as flativo "
      ",CASE WHEN b.filialcompraid is null "
            "THEN c.codfilialcad "
            "ELSE b.filialcompraid END as filialcompraid "
      ",CASE WHEN b.dsfilialcompra is null "
            "THEN c.dsfilialcad "
            "ELSE b.dsfilialcompra END as filialcompra "
      ",CASE WHEN s.Perfil_Compra is null "
            "THEN 'nao identificado' "
            "ELSE s.Perfil_Compra END as perfilcompra "
      ",udf_purchase_position( "
            "s.qtde_compra_mes, "
            "s.qtde_compra_12_meses, "
            "s.qtde_compra, "
            "udf_recency(s.dt_ultima_compra, '%s')) as statuscompra "
      ",udf_recency(s.dt_ultima_compra, '%s') as recencia "
      ",CASE WHEN s.qtde_compra is null "
            "THEN 0 "
            "ELSE s.qtde_compra END as qtd_compra "
      ",CASE WHEN s.vl_total_compra is null "
            "THEN 0 "
            "ELSE s.vl_total_compra END as vlr_compra "
      ",0 as qtd_item_compra "
      ",CASE WHEN s.qtde_compra_mes is null "
            "THEN 0 "
            "ELSE s.qtde_compra_mes END as qtd_compra_mes "
      ",CASE WHEN s.vl_total_compra_mes is null "
            "THEN 0 "
            "ELSE s.vl_total_compra_mes END as vlr_compra_mes "
      ",0 as qtd_item_compra_mes "
      ",CASE WHEN s.qtde_compra_12_meses is null "
            "THEN 0 "
            "ELSE s.qtde_compra_12_meses END as qtd_compra_12_meses "
      ",CASE WHEN s.vl_total_compra_12_meses is null "
            "THEN 0 "
            "ELSE s.vl_total_compra_12_meses END as vlr_compra_12_meses "
      ",0 as qtd_item_compra_12_meses "
      ",s.dt_primeira_compra "
      ",s.dt_ultima_compra "
      ",s.uc_meio_de_pagamento "
      ",s.uc_parcelas "
      ",c.uccanalecomm "
      ",c.ucsubcanalecomm "
      ",c.flcompraapp "
      ",udf_score( "
            "udf_recency(s.dt_ultima_compra, '%s'), "
            "s.qtde_compra_12_meses, "
            "s.vl_total_compra_12_meses) as score_cliente "
      ",udf_ltv( "
           "udf_score( "
               "udf_recency(s.dt_ultima_compra, '%s'), "
               "s.qtde_compra_12_meses, "
               "s.vl_total_compra_12_meses)) as ltv "
      ",CASE WHEN s.dt_ultima_compra is null THEN '' "
            "WHEN s.uc_parcelas is null THEN '' "
            "WHEN s.uc_parcelas == 0 THEN '' "
            "WHEN s.uc_parcelas == 1 and "
                 "s.uc_meio_de_pagamento != 'CARTAO DE CREDITO' and "
                 "s.uc_meio_de_pagamento != 'CARNE' THEN '' "
            "ELSE udf_add_months(s.dt_ultima_compra, cast(s.uc_parcelas as int)) "
            "END as dt_ultima_parcela "
      ",c.qtd_atendim_sac "
      ",c.qtd_dias_ult_reclam_sac "
      ",c.flatraso_maior_30dias "
      ",CASE WHEN s.flretiraloja is null "
            "THEN 0 ELSE s.flretiraloja END as flretiraloja "
  "from consolid_customer c "
       "LEFT OUTER JOIN "
       "purchase_branch_ds b "
           "on (c.id_gemco = b.id_gemco) "
       "LEFT OUTER JOIN "
       "customer_sales s "
           "on (c.id_gemco = s.id_gemco)" \
    % (dt_ult_dia_mes, dt_ult_dia_mes, dt_ult_dia_mes, dt_ult_dia_mes))

# Stores in Parquet format
single_consolidation.write.mode("overwrite") \
    .parquet("gs://prd-lake-trusted-atena/single_consolidation/")
