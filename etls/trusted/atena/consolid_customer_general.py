# coding: utf-8
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


# Starts the Spark Session
spark = SparkSession \
    .builder \
    .appName("CONSOLIDATION_CUSTOMER_GENERAL") \
    .enableHiveSupport() \
    .getOrCreate()

# Create a SQL Context
sc = spark.sparkContext
sqlContext = SQLContext(sc)


# Creates a temporary view using the DataFrame
sac_df = sqlContext.read.parquet("gs://prd-lake-raw-atena/sac_calls/")
sac_df.createOrReplaceTempView("sac_calls")

# Consolidation of the SAC infomations
consolid_sac_calls = spark.sql(
"select cpf_cnpj "
      ",count(distinct nro_protocolo) as qtd_atendim_sac "
      ",min(case when UPPER(desc_assunto) like '%RECLAM%' "
                "then datediff(now(), dt_atendimento) "
                "else null end) as qtd_dias_ult_reclam_sac "
  "from sac_calls "
 "group by cpf_cnpj")

# Creates a temporary view using the DataFrame
consolid_sac_calls.createOrReplaceTempView("consolid_sac_calls")


# Creates a temporary view using the DataFrame
charges_df = sqlContext.read \
    .parquet("gs://prd-lake-raw-atena/siscob_charges/")
charges_df.createOrReplaceTempView("siscob_charges")

# Consolidation of the Siscob charges infomations
consolid_siscob_charges = spark.sql(
"select codigo_cliente as id_gemco "
      ",max(datediff(now(), data_vencimento)) as qtd_dias_atraso "
      ",sum(valor_divida) as vlr_total_divida "
  "from siscob_charges "
 "group by codigo_cliente")

# Creates a temporary view using the DataFrame
consolid_siscob_charges.createOrReplaceTempView("consolid_siscob_charges")


# Creates a temporary view using the DataFrame
branch_df = sqlContext.read.parquet("gs://prd-lake-raw-atena/erp_branch/")
branch_df.createOrReplaceTempView("branch")


# Creates a temporary view using the DataFrame
employee_df = sqlContext.read.parquet("gs://prd-lake-raw-atena/employee/")
employee_df.createOrReplaceTempView("employee")


# Creates a temporary view using the DataFrame
single_df = sqlContext.read \
    .parquet("gs://prd-lake-trusted-atena/single_customer/")
single_df.createOrReplaceTempView("single_customer")


# Consolidation of the Customer infomations
consolid_customer = spark.sql(
"select c.cpf_cnpj "
      ",(case when c.id_gemco is null then cast('' as string) "
             "else cast(c.id_gemco as string) end) as id_gemco "
      ",(case when c.id_site is null then cast('' as string) "
             "when c.id_site == 'null' then cast('' as string) "
             "else cast(c.id_site as string) end) as id_site "
      ",c.uuid_site "
      ",(case when c.codfilialcad is null then cast('' as string) "
             "else cast(c.codfilialcad as string) end) as codfilialcad "
      ",h.dsfilial as dsfilialcad "
      ",(case when c.flclienteouro is null then 0 "
             "else c.flclienteouro end) as flclienteouro "
      ",0 as flgcartaoml " # MAPEAR POSTERIORMENTE
      ",(case when f.status is null then 0 "
             "else f.status end) as flfuncionario "
      ",0 as flcompraapp " # MAPEAR POSTERIORMENTE
      ",'' as uccanalecomm " # MAPEAR POSTERIORMENTE
      ",'' as ucsubcanalecomm " # MAPEAR POSTERIORMENTE
      ",(case when s.qtd_atendim_sac is null "
             "then cast('' as string) "
             "else cast(s.qtd_atendim_sac as string) "
        "end) as qtd_atendim_sac "
      ",(case when s.qtd_dias_ult_reclam_sac is null "
             "then cast('' as string) "
             "else cast(s.qtd_dias_ult_reclam_sac as string) "
        "end) as qtd_dias_ult_reclam_sac "
      ",(case when b.qtd_dias_atraso > 30 "
             "then cast('1' as string) "
             "else cast('0' as string) "
        "end) as flatraso_maior_30dias "
  "from single_customer c "
       "LEFT OUTER JOIN "
       "branch h "
           "ON (c.codfilialcad = h.codfil)"
       "LEFT OUTER JOIN "
       "(select cpf, status "
          "from employee "
         "where status = 1) f "
           "ON (c.cpf_cnpj = f.cpf) "
       "LEFT OUTER JOIN "
       "consolid_sac_calls s "
           "ON (c.cpf_cnpj = s.cpf_cnpj) "
       "LEFT OUTER JOIN "
       "consolid_siscob_charges b "
           "ON (c.id_gemco = b.id_gemco)")

# Stores in Parquet format
consolid_customer.write.mode("overwrite") \
    .parquet("gs://prd-lake-raw-atena/consolid_customer_general/")


# Creates a temporary view using the DataFrame
order_df = sqlContext.read.parquet("gs://prd-lake-raw-atena/erp_order/")
order_df.createOrReplaceTempView("order")


# Consolidation of the Branch infomations
consolid_purchase_branch = spark.sql(
"select cast(if(same_branch.codcli is null,diff_branch.codcli,same_branch.codcli) as int) id_gemco "
      ",if(qtde_same_branch >= if(qtde_diff_branch is null,0,qtde_diff_branch), same_branch.codfil, diff_branch.codfil) codfilcompra "
"from "
"(select max_q.codcli "
       ",min(q.codfil) as codfil "
       ",max_q.max_qty as qtde_diff_branch "
   "from (select codcli "
               ",max(qtde) max_qty "
           "from (select o.codcli "
                       ",o.codfil "
                       ",count(o.numpedprinc) as qtde "
                   "from order o "
                       ",single_customer c "
                  "where c.id_gemco = o.codcli "
                   " and (CASE WHEN o.codfil = c.codfilialcad THEN 1 ELSE 0 END) = 0 "
                  "group by o.codcli "
                          ",o.codfil) a "
          "group by codcli) max_q, "
        "(select o.codcli "
               ",o.codfil "
               ",count(o.numpedprinc) as qtde "
           "from order o "
               ",single_customer c "
          "where c.id_gemco = o.codcli "
            "and (CASE WHEN o.codfil = c.codfilialcad THEN 1 ELSE 0 END) = 0 "
          "group by o.codcli "
                  ",o.codfil) q "
  "where max_q.codcli = q.codcli "
    "and max_q.max_qty = q.qtde "
  "group by max_q.codcli, max_q.max_qty) diff_branch "
"FULL OUTER JOIN "
"(select o.codcli "
       ",o.codfil "
       ",count(o.numpedprinc) as qtde_same_branch "
   "from order o "
       ",single_customer c "
  "where c.id_gemco = o.codcli "
    "and (CASE WHEN o.codfil = c.codfilialcad THEN 1 ELSE 0 END) = 1 "
  "group by o.codcli "
          ",o.codfil) same_branch "
"ON diff_branch.codcli = same_branch.codcli")


# Stores in Parquet format
consolid_purchase_branch.write.mode("overwrite") \
    .parquet("gs://prd-lake-raw-atena/consolid_purchase_branch/")
