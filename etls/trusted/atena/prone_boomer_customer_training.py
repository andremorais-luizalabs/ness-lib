# coding: utf-8
from datetime import date
from datetime import datetime
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


# Starts the Spark Session
spark = SparkSession \
    .builder \
    .appName("PRONE_BOOMER_TRAINING") \
    .enableHiveSupport() \
    .getOrCreate()

# Create a SQL Context
sc = spark.sparkContext
sqlContext = SQLContext(sc)


# Creates a temporary view using the DataFrame
single_df = sqlContext.read \
    .parquet("gs://prd-lake-trusted-atena/single_customer/")

# Filtering just the necessary fields
new_single_df = single_df.selectExpr(
    "cpf_cnpj",
    "(case when numcartao = '' or numcartao is null "
          "then id_gemco "
          "else cast(numcartao as bigint) end) as numcartao",
    "dtcad",
    "dtnasc",
    "uf_tratado",
    "estcivil")

new_single_df.createOrReplaceTempView("single_customer")


# Creates a temporary view using the DataFrame
consolid_df = sqlContext.read \
    .parquet("gs://prd-lake-trusted-atena/single_consolidation/")
consolid_df.createOrReplaceTempView("single_consolidation")


# Creates a temporary view using the DataFrame
contact_df = sqlContext.read \
    .parquet("gs://prd-lake-raw-atena/boomer_contact/")
contact_df.createOrReplaceTempView("boomer_contact")


# Date of current jurisdiction
now = datetime.now().date()
dtjurisdict = now.replace(day=1)

# Geting the date of 18 months ago
dt_18_months_ago = dtjurisdict - timedelta(days=18 * 365 / 12)

# Geting the date of 3 months ago
dt_3_months_ago = dtjurisdict - timedelta(days=3 * 365 / 12)


# Captures customers who have already returned to a telemarketing contact
cust_already_return = spark.sql(
  """
  select s.cpf_cnpj as id_single
        ,max(b.dtacontato) as dtinteracted
        ,'contacts' as tpinteracted
    from boomer_contact b
         INNER JOIN
         single_customer s
           ON (s.numcartao = b.cartao)
   where b.intidcontato in (3, 102)
     and b.intidpontuacao is not null
     and b.dtacontato >= '%s'
     and b.dtacontato < '%s'
   group by s.cpf_cnpj
   """ % (dt_18_months_ago, dtjurisdict))

# Creates a temporary view using the DataFrame
cust_already_return.createOrReplaceTempView("cust_already_return")


# Captures customers who never returned to a telemarketing contact
cust_never_return = spark.sql(
  """
  select s.cpf_cnpj as id_single,
         max(b.dtacontato) as dtinteracted,
         'contacts' as tpinteracted
    from (select n.cartao, n.dtacontato
            from
              (select cartao, dtacontato
                 from boomer_contact
                where intidcontato in (3, 102)
                  and intidpontuacao is null
                  and dtacontato >= '%s'
                  and dtacontato < '%s') n
              LEFT OUTER JOIN
              (select cartao, dtacontato
                 from boomer_contact
                where intidcontato in (3, 102)
                  and intidpontuacao is not null) c
                ON (n.cartao = c.cartao)
           where c.cartao is null) b
         INNER JOIN
         single_customer s
           ON (s.numcartao = b.cartao)
   where s.dtcad <= '%s'
   group by s.cpf_cnpj
  """ % (dt_3_months_ago, dtjurisdict, dt_18_months_ago))

# Creates a temporary view using the DataFrame
cust_never_return.createOrReplaceTempView("cust_never_return")


# Sample of customers prone to telemarketing
cons_cust_already_return = spark.sql(
  """
  select distinct p.*
     from single_consolidation p
          INNER JOIN
          (select t.id_single, t.dtinteracted
            from (select id_single
                        ,max(dtinteracted) as dtinteracted
                    from cust_already_return
                   group by id_single) t
           order by t.dtinteracted desc
           limit 600000) m
              ON (p.id_single = m.id_single)
  """)

# Creates a temporary view using the DataFrame
cons_cust_already_return.createOrReplaceTempView("cons_cust_already_return")


# Sample of customers not prone to telemarketing
cons_cust_never_return = spark.sql(
  """
  select distinct p.*
    from single_consolidation p
         INNER JOIN
         (select t.id_single, t.dtinteracted
            from (select id_single
                        ,max(dtinteracted) as dtinteracted
                    from cust_never_return
                   group by id_single) t
           order by t.dtinteracted desc
           limit 700000) a
         ON (p.id_single = a.id_single)
  """)

# Creates a temporary view using the DataFrame
cons_cust_never_return.createOrReplaceTempView("cons_cust_never_return")


# Calculate of born date
def calc_age(born):

    if not born:
        return 0

    try:
        if (len(born) == 10):
            born = datetime.strptime(born, '%Y-%m-%d')
        else:
            born = datetime.strptime(born, '%Y-%m-%d %H:%M:%S')
    except:
        return 0

    today = date.today()
    return today.year - born.year - \
        ((today.month, today.day) < (born.month, born.day))

sqlContext.udf.register("udf_calc_age", calc_age)


# Calculate of difference in months
def months_diff(initialdate):

    if not initialdate:
        return 0

    try:
        if (len(initialdate) == 10):
            initialdate = datetime.strptime(initialdate, '%Y-%m-%d')
        else:
            initialdate = datetime.strptime(initialdate, '%Y-%m-%d %H:%M:%S')
    except:
        return 0

    dtoday = date.today()
    months = (dtoday.year - initialdate.year) * 12 + \
        (dtoday.month - initialdate.month)
    return months

sqlContext.udf.register("udf_months_diff", months_diff)


# Generates training file
prone_training = spark.sql(
  """
  select concat('id_', c.cpf_cnpj) as id
        ,udf_calc_age(c.dtnasc) as idade
        ,(CASE WHEN c.uf_tratado = 'AC' THEN 1  WHEN c.uf_tratado = 'AL' THEN 2
               WHEN c.uf_tratado = 'AM' THEN 3  WHEN c.uf_tratado = 'AP' THEN 4
               WHEN c.uf_tratado = 'BA' THEN 5  WHEN c.uf_tratado = 'CE' THEN 6
               WHEN c.uf_tratado = 'DF' THEN 7  WHEN c.uf_tratado = 'ES' THEN 8
               WHEN c.uf_tratado = 'GO' THEN 9  WHEN c.uf_tratado = 'MA' THEN 10
               WHEN c.uf_tratado = 'MG' THEN 11 WHEN c.uf_tratado = 'MS' THEN 12
               WHEN c.uf_tratado = 'MT' THEN 13 WHEN c.uf_tratado = 'PA' THEN 14
               WHEN c.uf_tratado = 'PB' THEN 15 WHEN c.uf_tratado = 'PE' THEN 16
               WHEN c.uf_tratado = 'PI' THEN 17 WHEN c.uf_tratado = 'PR' THEN 18
               WHEN c.uf_tratado = 'RJ' THEN 19 WHEN c.uf_tratado = 'RN' THEN 20
               WHEN c.uf_tratado = 'RO' THEN 21 WHEN c.uf_tratado = 'RR' THEN 22
               WHEN c.uf_tratado = 'RS' THEN 23 WHEN c.uf_tratado = 'SC' THEN 24
               WHEN c.uf_tratado = 'SE' THEN 25 WHEN c.uf_tratado = 'SP' THEN 26
               WHEN c.uf_tratado = 'TO' THEN 27 ELSE 0 END) as uf
        ,(CASE WHEN c.estcivil = 'DIVORCIADO'    THEN 1
               WHEN c.estcivil = 'UNIAO ESTAVEL' THEN 2
               WHEN c.estcivil = 'SOLTEIRO'      THEN 3
               WHEN c.estcivil = 'SEPARADO'      THEN 4
               WHEN c.estcivil = 'VIUVO'         THEN 5
               WHEN c.estcivil = 'CASADO'        THEN 6
               ELSE 0 END) as estcivil
        ,(CASE WHEN p.flclienteouro = 1
               THEN 1
               ELSE 0 END) as flclienteouro
        ,(CASE WHEN p.dt_primeira_compra is null
               THEN 0
               ELSE udf_months_diff(p.dt_primeira_compra)
               END) as primeiracompra
        ,(CASE WHEN p.uc_meio_de_pagamento = 'CARTAO DE CREDITO' THEN 1
               WHEN p.uc_meio_de_pagamento = 'BOLETO'            THEN 2
               WHEN p.uc_meio_de_pagamento = 'CARTAO DE DEBITO'  THEN 3
               WHEN p.uc_meio_de_pagamento = 'CARNE'             THEN 4
               WHEN p.uc_meio_de_pagamento = 'OUTROS'            THEN 5
               WHEN p.uc_meio_de_pagamento = 'CHEQUE'            THEN 6
               WHEN p.uc_meio_de_pagamento = 'DINHEIRO'          THEN 7
               WHEN p.uc_meio_de_pagamento = 'CREDDEVOL'         THEN 8
               WHEN p.uc_meio_de_pagamento = 'CARTA CONSORCIO'   THEN 9
               ELSE 0 END) as ucmeiopagto
        ,(CASE WHEN p.uc_parcelas is null
               THEN 0
               ELSE p.uc_parcelas END) as ucparcelas
        ,p.recencia
        ,p.score_cliente as score
        ,(CASE WHEN p.perfilcompra = 'loja' THEN 1
               WHEN p.perfilcompra = 'site' THEN 2
               WHEN p.perfilcompra = 'multicanal' THEN 3
               ELSE 0 END) as perfilcompra
        ,p.qtd_compra as qtcomprastotal
        ,ROUND(p.vlr_compra, 2) as vlcomprastotal
        ,(CASE WHEN p.qtd_compra is null or p.qtd_compra = 0
               THEN 0
               ELSE ROUND( ROUND(p.vlr_compra, 2) / p.qtd_compra, 2)
          END) as vlticketmedio
        ,1 as flpropenso
    from single_customer c
         INNER JOIN
         cons_cust_already_return p
         ON (c.cpf_cnpj = p.id_single)
  UNION
  select concat('id_', c.cpf_cnpj) as id
        ,udf_calc_age(c.dtnasc) as idade
        ,(CASE WHEN c.uf_tratado = 'AC' THEN 1  WHEN c.uf_tratado = 'AL' THEN 2
               WHEN c.uf_tratado = 'AM' THEN 3  WHEN c.uf_tratado = 'AP' THEN 4
               WHEN c.uf_tratado = 'BA' THEN 5  WHEN c.uf_tratado = 'CE' THEN 6
               WHEN c.uf_tratado = 'DF' THEN 7  WHEN c.uf_tratado = 'ES' THEN 8
               WHEN c.uf_tratado = 'GO' THEN 9  WHEN c.uf_tratado = 'MA' THEN 10
               WHEN c.uf_tratado = 'MG' THEN 11 WHEN c.uf_tratado = 'MS' THEN 12
               WHEN c.uf_tratado = 'MT' THEN 13 WHEN c.uf_tratado = 'PA' THEN 14
               WHEN c.uf_tratado = 'PB' THEN 15 WHEN c.uf_tratado = 'PE' THEN 16
               WHEN c.uf_tratado = 'PI' THEN 17 WHEN c.uf_tratado = 'PR' THEN 18
               WHEN c.uf_tratado = 'RJ' THEN 19 WHEN c.uf_tratado = 'RN' THEN 20
               WHEN c.uf_tratado = 'RO' THEN 21 WHEN c.uf_tratado = 'RR' THEN 22
               WHEN c.uf_tratado = 'RS' THEN 23 WHEN c.uf_tratado = 'SC' THEN 24
               WHEN c.uf_tratado = 'SE' THEN 25 WHEN c.uf_tratado = 'SP' THEN 26
               WHEN c.uf_tratado = 'TO' THEN 27 ELSE 0 END) as uf
        ,(CASE WHEN c.estcivil = 'DIVORCIADO'    THEN 1
               WHEN c.estcivil = 'UNIAO ESTAVEL' THEN 2
               WHEN c.estcivil = 'SOLTEIRO'      THEN 3
               WHEN c.estcivil = 'SEPARADO'      THEN 4
               WHEN c.estcivil = 'VIUVO'         THEN 5
               WHEN c.estcivil = 'CASADO'        THEN 6
               ELSE 0 END) as estcivil
        ,(CASE WHEN p.flclienteouro = 1
               THEN 1
               ELSE 0 END) as flclienteouro
        ,(CASE WHEN p.dt_primeira_compra is null
               THEN 0
               ELSE udf_months_diff(p.dt_primeira_compra)
               END) as primeiracompra
        ,(CASE WHEN p.uc_meio_de_pagamento = 'CARTAO DE CREDITO' THEN 1
               WHEN p.uc_meio_de_pagamento = 'BOLETO'            THEN 2
               WHEN p.uc_meio_de_pagamento = 'CARTAO DE DEBITO'  THEN 3
               WHEN p.uc_meio_de_pagamento = 'CARNE'             THEN 4
               WHEN p.uc_meio_de_pagamento = 'OUTROS'            THEN 5
               WHEN p.uc_meio_de_pagamento = 'CHEQUE'            THEN 6
               WHEN p.uc_meio_de_pagamento = 'DINHEIRO'          THEN 7
               WHEN p.uc_meio_de_pagamento = 'CREDDEVOL'         THEN 8
               WHEN p.uc_meio_de_pagamento = 'CARTA CONSORCIO'   THEN 9
               ELSE 0 END) as ucmeiopagto
        ,(CASE WHEN p.uc_parcelas is null
               THEN 0
               ELSE p.uc_parcelas END) as ucparcelas
        ,p.recencia
        ,p.score_cliente as score
        ,(CASE WHEN p.perfilcompra = 'loja' THEN 1
               WHEN p.perfilcompra = 'site' THEN 2
               WHEN p.perfilcompra = 'multicanal' THEN 3
               ELSE 0 END) as perfilcompra
        ,p.qtd_compra as qtcomprastotal
        ,ROUND(p.vlr_compra, 2) as vlcomprastotal
        ,(CASE WHEN p.qtd_compra is null or p.qtd_compra = 0
               THEN 0
               ELSE ROUND( ROUND(p.vlr_compra, 2) / p.qtd_compra, 2)
          END) as vlticketmedio
        ,0 as flpropenso
   from single_customer c
        INNER JOIN
        cons_cust_never_return p
            ON (c.cpf_cnpj = p.id_single)
  """)


# Writing the consolidation Customers for channels and Periodicity in files
prone_training.repartition(5).write.csv(
    "gs://prd-lake-trusted-atena/boomerang_prone/training/",
    sep="|", mode="overwrite")
