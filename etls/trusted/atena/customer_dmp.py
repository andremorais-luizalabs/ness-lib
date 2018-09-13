# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *


# Starts the Spark Session
spark = SparkSession \
    .builder \
    .appName("EXPORT_DMP") \
    .enableHiveSupport() \
    .getOrCreate()

# Create a SQL Context
sc = spark.sparkContext
sqlContext = SQLContext(sc)


# Creates a temporary view using the DataFrame
single_df = sqlContext.read \
    .parquet("gs://prd-lake-trusted-atena/single_customer/")
single_df.createOrReplaceTempView("single_customer")


# Creates a temporary view using the DataFrame
consolid_df = sqlContext.read \
    .parquet("gs://prd-lake-trusted-atena/single_consolidation/")
consolid_df.createOrReplaceTempView("single_consolidation")


# Creates a temporary view using the DataFrame
gc_df = sqlContext.read \
    .parquet("gs://prd-lake-raw-atena/gc_customer/")
gc_df.createOrReplaceTempView("gc_customer")


# Informations for DMP exportation
dmp_df = spark.sql(
"select c.uuid_site "
      ",(case when d.statuscompra is null then 'nao comprou' "
             "when d.statuscompra = '' then 'nao comprou' "
             "else d.statuscompra "
             "end) as tipodecliente "
      ",(case when c.estcivil is null then 'null' "
             "when c.estcivil = '' then 'null' "
             "else LOWER(c.estcivil) "
             "end) as estadocivil "
      ",(case when c.sexo = 'F' then 'feminino' "
             "when c.sexo = 'M' then 'masculino' "
             "else 'null' "
             "end) as sexo "
      ",LOWER(d.ltv) as ltv "
      ",(case when g.modelo_quem is null then 'null' "
             "when g.modelo_quem = '' then 'null' "
             "else LOWER(g.modelo_quem) "
             "end) as modeloquem "
      ",(case when g.cartaoml = 1 "
             "then 'sim' "
             "else 'nao' "
             "end) as cartaoluiza "
      ",(case when (case when g.fl_clienteouro20 is null then 0 "
                        "when g.fl_clienteouro20 == 1 then 1 "
                        "else 0 end) == 1 then 'sim' "
             "when d.flclienteouro is null then 'nao' "
             "when d.flclienteouro == 1 then 'sim' "
             "else 'nao' "
             "end) as clienteouro "
      ",(case when d.perfilcompra is null then 'nao comprou' "
             "when d.perfilcompra = '' then 'nao comprou' "
             "else d.perfilcompra "
             "end) as canaldocliente "
      ",(case when d.dt_ultima_compra is null then 'null' "
             "else substr(cast(d.dt_ultima_compra as string),1,10) "
             "end) as dtultimacompra "
      ",(case when d.filialcompraid = 200 then lower(c.uf_tratado) "
             "when d.filialcompra is null then 'null' "
             "when d.filialcompra = '' then 'null' "
             "else LOWER(substr(substr(d.filialcompra,6,length(d.filialcompra)), "
                            "instr(substr(d.filialcompra,6,length(d.filialcompra)),'-')+1, "
                                  "length(substr(d.filialcompra,6,length(d.filialcompra))))) "
             "end) estadodacompra "
      ",(case when d.filialcompraid = 200 then lower(c.cidade_tratado) "
             "when d.filialcompra is null then 'null' "
             "when d.filialcompra = '' then 'null' "
             "else LOWER(substr(substr(d.filialcompra,6,length(d.filialcompra)),1, "
                           "instr(substr(d.filialcompra,6,length(d.filialcompra)),'-')-1)) "
             "end) as cidadedacompra "
      ",(case when d.filialcompraid is null then 'null' "
             "else cast(d.filialcompraid as string) "
             "end) as filialcompra "
      ",(case when d.uc_parcelas is null then 'null' "
             "else cast(d.uc_parcelas as string) "
             "end) as parcelas "
      ",(case when d.uc_meio_de_pagamento is null then 'null' "
             "when d.uc_meio_de_pagamento = '' then 'null' "
             "else LOWER(d.uc_meio_de_pagamento) "
             "end) as formadepagamento "
      ",(case when d.dt_ultima_parcela = '' then 'null' "
             "else (case month(d.dt_ultima_parcela) when 1 then 'janeiro' "
                                                   "when 2 then 'fevereiro' "
                                                   "when 3 then 'marco' "
                                                   "when 4 then 'abril' "
                                                   "when 5 then 'maio' "
                                                   "when 6 then 'junho' "
                                                   "when 7 then 'julho' "
                                                   "when 8 then 'agosto' "
                                                   "when 9 then 'setembro' "
                                                   "when 10 then 'outubro' "
                                                   "when 11 then 'novembro' "
                                                   "when 12 then 'dezembro' end) "
             "end) as mesultimaparcela "
      ",(case when d.dt_ultima_parcela = '' then 'null' "
             "else cast(year(d.dt_ultima_parcela) as string) "
             "end) as anoultimaparcela "
      ",(case when d.recencia is null then 'null' "
             "else cast(d.recencia as string) "
             "end) as mesessemcompra "
      ",(case when c.dtnasc is null then 'null' "
             "else cast(year(c.dtnasc) as string) "
             "end) as anonascimento "
      ",(case when c.dtnasc is null then 'null' "
             "else (case month(c.dtnasc) when 1 then 'janeiro' "
                                        "when 2 then 'fevereiro' "
                                        "when 3 then 'marco' "
                                        "when 4 then 'abril' "
                                        "when 5 then 'maio' "
                                        "when 6 then 'junho' "
                                        "when 7 then 'julho' "
                                        "when 8 then 'agosto' "
                                        "when 9 then 'setembro' "
                                        "when 10 then 'outubro' "
                                        "when 11 then 'novembro' "
                                        "when 12 then 'dezembro' end) "
             "end) as mesnascimento "
      ",(case when c.dtnasc is null then 'null' "
             "else cast(day(c.dtnasc) as string) "
             "end) as dianascimento "
      ",(case when c.optin_email_site is null then '0' "
             "when c.optin_email_site = 'false' then '0' "
             "else '1' "
             "end) as optinemail "
      ",(case when d.qtd_atendim_sac = '' then 'null' "
             "else cast(d.qtd_atendim_sac as string) "
             "end) as qtdatendimsac "
      ",(case when d.qtd_dias_ult_reclam_sac = '' then 'null' "
             "else cast(d.qtd_dias_ult_reclam_sac as string) "
             "end) as qtddiasultreclamsac "
      ",(case when d.flatraso_maior_30dias is null then '0' "
             "else cast(d.flatraso_maior_30dias as string) "
             "end) as flatrasomaior30dias "
      ",(case when fl_comprouecom_2017 is null then 'nao' "
             "when fl_comprouecom_2017 == 1 then 'sim' "
             "else 'nao' "
             "end) as comprouecom2017 "
      ",(case when fl_comprouecom_2018 is null then 'nao' "
             "when fl_comprouecom_2018 == 1 then 'sim' "
             "else 'nao' "
             "end) as comprouecom2018 "
      ",(case when clv is null or clv == '' then 'null' "
             "else substr(clv, 0, instr(clv,',')-1) "
             "end) as clv "
      ",(case when segmento_clv is null or segmento_clv == '' "
             "then 'null' "
             "else segmento_clv "
             "end) as segmento_clv "
    "from single_customer c "
       "INNER JOIN "
       "single_consolidation d "
           "on (c.cpf_cnpj = d.id_single) "
       "LEFT OUTER JOIN "
       "gc_customer g "
           "on (c.cpf_cnpj = g.cpf)") \
    .fillna('null') \
    .filter("length(uuid_site)>0 and uuid_site<>'null'")


# Name fields to layout DMP
FIELDS = ('tipodecliente',
          'estadocivil',
          'sexo',
          'ltv',
          'modeloquem',
          'cartaoluiza',
          'clienteouro',
          'canaldocliente',
          'dtultimacompra',
          'estadodacompra',
          'cidadedacompra',
          'filialcompra',
          'parcelas',
          'formadepagamento',
          'mesultimaparcela',
          'anoultimaparcela',
          'mesessemcompra',
          'anonascimento',
          'mesnascimento',
          'dianascimento',
          'optinemail',
          'qtdatendimsac',
          'qtddiasultreclamsac',
          'flatrasomaior30dias',
          'comprouecom2017',
          'comprouecom2018',
          'clv',
          'segmento_clv')


# Creating UDF for layout DMP
def layout(tuplefields):
    """
    Mounts the fields string of the layout

    """
    ret = ''
    max_index = len(FIELDS)-1

    # String of the layout
    for i in range(0, max_index+1):

        if i < max_index:
            ret = ret + '"' + FIELDS[i] + '"' + '="' + tuplefields[i] + '",'
        else:
            ret = ret + '"' + FIELDS[i] + '"' + '="' + tuplefields[i] + '"'

    return ret

udf_layout = udf(layout)


# Creating a tuple with fields of the layout
export = dmp_df.withColumn("fields", udf_layout(struct(*FIELDS))) \
    .select("uuid_site", "fields")


# Writing for files
export.repartition(5).write.option("quote", "\u0000").csv(
    "gs://prd-lake-trusted-atena/export/dmp/",
    sep="\t", mode="overwrite")
