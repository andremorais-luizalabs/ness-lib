# coding: utf-8

# In[3]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime
from unicodedata import normalize, category
from functools import reduce

# In[4]:


# # Start Spark Session
# spark = SparkSession\
# .builder\
# .appName("GEMCO_CUSTOMER_IMPORTER")\
# .getOrCreate()


spark = SparkSession.builder.appName("GEMCO_CUSTOMER_IMPORTER").getOrCreate()

# In[5]:


# UDFs Declaration
# https://docs.databricks.com/spark/latest/spark-sql/udf-in-python.html
import re

RE_CPF = re.compile(r'(?:\d{11})')
RE_CNPJ = re.compile(r'(?:\d{14})')


def valid_cgccpf(tp_person, cgccpf):
    if not cgccpf:
        return None

    if tp_person == 'F':
        return cpf(cgccpf)
    elif tp_person == 'J':
        return cnpj(cgccpf)
    else:
        return None


def cpf(cpf):
    cpf = ''.join(RE_CPF.findall(str(cpf)))

    if (not cpf) or (len(cpf) < 11) or cpf in ('00000000000', ''):
        return None
    # Pega apenas os 9 primeiros dígitos do CPF e gera os 2 dígitos que faltam
    # inteiros = map(int, cpf)
    inteiros = list(map(int, cpf))
    novo = inteiros[:9]
    if not novo:
        return None
    while len(novo) < 11:

        a = [(len(novo) + 1 - i) * v for i, v in enumerate(novo)]
        r = reduce(lambda x, y: x + y, a) % 11
        if r > 1:
            f = 11 - r
        else:
            f = 0
        novo.append(f)
    # Se o número gerado coincidir com o número original, é válido
    if novo == inteiros:
        return 1
    return 0


def cnpj(cnpj):
    cnpj = ''.join(RE_CNPJ.findall(str(cnpj)))

    if (not cnpj) or (len(cnpj) < 14) or cnpj == '00000000000000':
        return 0

    # Pega apenas os 12 primeiros dígitos do CNPJ e gera os 2
    # dígitos que faltam
    inteiros = list(map(int, cnpj))
    novo = inteiros[:12]

    prod = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    while len(novo) < 14:
        a = [x * y for (x, y) in zip(novo, prod)]
        r = reduce(lambda x, y: x + y, a) % 11
        if r > 1:
            f = 11 - r
        else:
            f = 0
        novo.append(f)
        prod.insert(0, 6)

    # Se o número gerado coincidir com o número original, é válido
    if novo == inteiros:
        return 1
    return 0


def remove_special_characters(str_input):
    """
    Remove special characters entered of the string
    - str_input: String to clear special characters
    """
    if str_input is None:
        return ''
    str_input = str_input.replace('\\', ' ').replace('\x09', ' ').replace('\x5C', ' ').replace('  ', ' ').strip()
    return str_input


remove_special = udf(remove_special_characters)


def valid_syntax_email(str_email):
    """
    Valide email.
    - Parameter: string type
    - Return (int type): 1-Ok and 0-Nok
    """

    match = re.match(r'^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+'
                     r'(\.[a-z0-9-]+)*(\.[a-z]{2,4})$', str(str_email))

    if match:
        return 1
    else:
        return 0


valid_email_udf = udf(valid_syntax_email)


def remove_accents(str_input):
    """
    Remove accents of the string
    """
    if not str_input:
        return None

    return ''.join((c for c in normalize('NFD', str_input) if category(c) != 'Mn'))


remove_accents_udf = udf(remove_accents)

estado_civil = {('NOIVO' 'SOLTEIRO'): 'SOLTEIRO',
                'CASADO': 'CASADO',
                'SEPARADO': 'SEPARADO',
                ('VI�VO', 'VIUVO'): 'VIUVO',
                'DIVORCIADO': 'DIVORCIADO',
                'UNIAO ESTAV': 'UNIAO ESTAVEL'}

sit_cred = {'RI': 'CREDITO DE RISCO',
            'NO': 'SITUACAO DE VENDA NORMAL',
            'NE': 'NEGATIVADO SPC',
            'RE': 'CREDITO RENEGOCIADO',
            'CC': 'CANCELADO PELO CLIENTE',
            'CE': 'CANCELADO EMPRESA',
            'PV': 'PROIBIDA VENDA',
            'FL': 'OUTRAS OCORRENCIAS',
            'FA': 'CLIENTE FALECIDO',
            'CH': 'CLIENTE COM CHEQUE ZERO',
            'GO': 'SUSPEITO GOLPISTA',
            'DP': 'CAD INVALIDO - LOCALIZAR OUTRO',
            'FI': 'CONFIRMAR SE CPF ESTA CORRETO',
            'DE': 'LIB FUNC X',
            'FU': 'CLIENTE FUNCIONARIO',
            'BL': 'BLACK LIST FRAUDE'}

devcorresp = {1: 'MUDOU-SE',
              2: 'ENDERECO INSUFICIENTE',
              3: 'NAO EXISTE NUMERO INDICADO',
              4: 'BAIRRO SEM CORREIO',
              5: 'CEP ERRADO / INCOMPLETO',
              6: 'DESCONHECIDO',
              7: 'TELEFONE ERRADO',
              8: 'NAO PROCURADO',
              9: 'AUSENTE',
              10: 'FALECIDO'}

states = {'AC': 'ACRE', 'AL': 'ALAGOAS', 'AP': 'AMAPA', 'AM': 'AMAZONAS',
          'BA': 'BAHIA', 'CE': 'CEARA', 'DF': 'DISTRITO FEDERAL',
          'ES': 'ESPIRITO SANTO', 'GO': 'GOIAS', 'MA': 'MARANHAO',
          'MT': 'MATO GROSSO', 'MS': 'MATO GROSSO DO SUL',
          'MG': 'MINAS GERAIS', 'PA': 'PARA', 'PB': 'PARAIBA',
          'PR': 'PARANA', 'PE': 'PERNAMBUCO', 'PI': 'PIAUI',
          'RJ': 'RIO DE JANEIRO', 'RN': 'RIO GRANDE DO NORTE',
          'RS': 'RIO GRANDE DO SUL', 'RO': 'RONDONIA', 'RR': 'RORAIMA',
          'SC': 'SANTA CATARINA', 'SP': 'SAO PAULO', 'SE': 'SERGIPE',
          'TO': 'TOCANTINS'}

estado_civil_udf = udf(lambda x: estado_civil.get(x, "") if x else "")
sit_cred_udf = udf(lambda x: sit_cred.get(x, None) if x else "")
devcorresp_udf = udf(lambda x: devcorresp.get(x, "") if x else "")
sexo_udf = udf(lambda x: x if x in ("M", "F") else "")
flcorrespond_udf = udf(lambda x: 1 if x == "S" else 0)
flativo_udf = udf(lambda x: 0 if x == 9 else 1)
default_for_null = udf(lambda x: "NAO INFORMADO" if not x else x)
domain_state_address = udf(lambda x: states.get(x, "") if x else "")
valid_cgccpf_udf = udf(valid_cgccpf)

# In[6]:


destination_customer = "gs://prd-lake-raw-atena/gemco_cliente/"
destination_cep = "gs://prd-lake-raw-atena/gemco_cliente_cep/"
destination_addrs = "gs://prd-lake-raw-atena/gemco_cliente_addr/"
destination_card = "gs://prd-lake-raw-atena/gemco_cliente_card/"

# In[7]:


# IMPORT CUSTOMER TRANSIENT

path_customer = "gs://prd-lake-transient-atena/atena/gemco_cliente/"
schema = spark.read.option("delimiter", "|").csv(path_customer).schema
cust_df = spark.readStream.option("delimiter", "|").csv(path_customer, schema=schema)

# In[8]:


# Renaming and transforming columns

timmed_df = cust_df.select([upper(trim(col(col_name))).alias(col_name) for col_name in cust_df.columns])
customer = timmed_df.select(
    col("_c0").alias("origem"),
    col("_c1").alias("codcli"),
    col("_c2").alias("digcli"),
    col("_c3").alias("natjur"),
    col("_c4").alias("dtcadast"),
    col("_c5").alias("tsultalt"),
    remove_accents_udf("_c6").alias("nomcli"),
    remove_accents_udf("_c7").alias("fantasia"),
    col("_c8").alias("dtnasc"),
    col("_c9").alias("cgccpf"),
    valid_cgccpf_udf(col("_c3"), col("_c9")).alias("flvalidcgccpf"),
    col("_c10").alias("numdoc"),
    upper(col("_c11")).alias("emissor"),
    col("_c12").alias("sexo"),
    sexo_udf("_c12").alias("sexo_tratado"),
    col("_c13").alias("codestcivil"),
    estado_civil_udf("_c14").alias("estcivil"),
    remove_accents_udf("_c15").alias("nomepai"),
    remove_accents_udf("_c16").alias("nomemae"),
    col("_c17").alias("codfilcad"),
    col("_c18").alias("temporesidmes"),
    col("_c19").alias("status"),
    flativo_udf("_c19").alias("flativo"),
    col("_c20").alias("codcargo"),
    remove_accents_udf("_c21").alias("cargo"),
    col("_c22").alias("vlrendbruto"),
    col("_c23").alias("vloutrendas"),
    remove_accents_udf("_c24").alias("nomeconjuge"),
    col("_c25").alias("dtnascconj"),
    col("_c26").alias("dddconj"),
    col("_c27").alias("foneconj"),
    col("_c28").alias("rendaconj"),
    col("_c29").alias("dddrefcoml1"),
    col("_c30").alias("fonerefcoml1"),
    col("_c31").alias("dtultmcomp"),
    col("_c32").alias("dtatucad"),
    col("_c33").alias("codsitcredneg"),
    sit_cred_udf("_c33").alias("sitcredneg"),
    col("_c35").alias("flpresenteado"),
    col("_c36").alias("nrdependentes"),
    col("_c37").alias("codnatprof"),
    upper(col("_c38")).alias("descnatprof"),
    col("_c39").alias("cpfconjuge"),
    col("_c40").alias("flnaocontactar"),
    col("_c41").alias("coddevcorresp"),
    devcorresp_udf("_c41").alias("devcorresp"),
    col("_c42").alias("vltotcompras"),
    col("_c43").alias("qtcomprasprazoliq"),
    col("_c44").alias("compprazoqtde"),
    col("_c45").alias("compprazovl"),
    col("_c46").alias("compvistaqtde"),
    (col("_c44") + col("_c46")).alias("qtcompras"),
    col("_c47").alias("compvistavl"),
    col("_c48").alias("diasmedatraso"),
    col("_c49").alias("maioratraso"),
    col("_c50").alias("maiorcmpqtde"),
    col("_c51").alias("maiorcmpvalor"),
    col("_c52").alias("maiorplnqtde"),
    col("_c53").alias("maiorplnvalor"),
    col("_c54").alias("nrlpaberto"),
    col("_c55").alias("nrlptotal"),
    col("_c56").alias("nrspcaberto"),
    col("_c57").alias("nrtotspc"),
    col("_c58").alias("vlmaiorcompra"),
    col("_c59").alias("compnaoaprov"),
    col("_c60").alias("ultresultspc"),
    col("_c61").alias("dtultconsspc")) \
    .withColumn("etl_id", lit(str(datetime.date.today()))) \
 \
# In[9]:


destination_customer = "gs://prd-lake-raw-atena/gemco_cliente/"
custom_squery = customer.writeStream.partitionBy("etl_id").format("parquet").trigger(once=True).option(
    "checkpointLocation", "gs://prd-lake-transient-atena/checkpoints/crm_gemco_cliente/").option("path",
                                                                                                 destination_customer).start()

custom_squery.awaitTermination()

# In[10]:


# IMPORT CEP
path_cep = "gs://prd-lake-transient-atena/atena/cep_correio/"
cep_df = spark.read.option("delimiter", "|").csv(path_cep)

# In[11]:


cep_column_names = ["cep_cep", "uf_cep", "cidade_cep", "bairro_cep",
                    "codlograd_cep", "lograd_cep", "endereco_cep",
                    "de_cep", "ate_cep", "tipo_cep"]

cep = cep_df.select([remove_special(remove_accents_udf(upper(col(y)))).alias(cep_column_names[x])
                     for x, y in enumerate(cep_df.columns)]) \
    .withColumn("etl_id", lit(str(datetime.date.today())))

# In[12]:


destination_cep = "gs://prd-lake-raw-atena/gemco_cliente_cep/"
add_custom_squery = cep.write.mode("overwrite").parquet(destination_cep)

# In[13]:


# IMPORT ADDRESS
path = "gs://prd-lake-transient-atena/crm/crm_gemco_cliente_ender/"
schema = spark.read.option("delimiter", "|").csv(path).schema
addr_df = spark.readStream.option("delimiter", "|").csv(path, schema=schema)
# addr_df = spark.read.option("delimiter", "|").csv(path)
# applying functions: remove_accents, upper, trim
address_treated = addr_df.select(
    [default_for_null(remove_accents_udf(upper(trim(col(col_name))))).alias(col_name) for col_name in addr_df.columns])

# Renaming and transforming columns
address_renamed = address_treated.select(
    col("_c1").alias("codcli"),
    col("_c2").alias("codend"),
    col("_c3").alias("tpender"),
    col("_c4").alias("cep"),
    col("_c5").alias("codlograd"),
    col("_c6").alias("lograd"),
    col("_c7").alias("endereco"),
    col("_c8").alias("numero"),
    col("_c9").alias("complemento"),
    col("_c10").alias("cidade"),
    col("_c11").alias("uf"),
    domain_state_address("_c11").alias("estado"),
    col("_c12").alias("bairro"),
    lower(col("_c13")).alias("e_mail"),
    valid_email_udf(lower(col("_c13"))).alias("flvalidemail"),
    col("_c14").alias("tpresid"),
    col("_c15").alias("tpresidencia"),
    flcorrespond_udf("_c16").alias("flcorrespond"),
    col("_c17").alias("dddfone"),
    col("_c18").alias("fone"),
    col("_c19").alias("dddcel"),
    col("_c20").alias("telcel")) \
    .withColumn('endereco_completo', concat(col("endereco") + lit("") + col("complemento"))) \
    .withColumn("etl_id", lit(str(datetime.date.today())))

# In[14]:


destination_addrs = "gs://prd-lake-raw-atena/gemco_cliente_addr/"
add_custom_squery = address_renamed.writeStream.partitionBy("etl_id").format("parquet").trigger(once=True).option(
    "checkpointLocation", "gs://prd-lake-transient-atena/checkpoints/crm_gemco_cliente_addr/").option("path",
                                                                                                      destination_addrs).start().awaitTermination()

# In[15]:


# Imports card
path = "gs://prd-lake-transient-atena/atena/gemco_cliente_cartao/"
schema = spark.read.option("delimiter", "|").csv(path).schema
card_df = spark.readStream.option("delimiter", "|").csv(path, schema=schema)
# card_df = spark.read.option("delimiter", "|").csv(path)

# Renaming and transforming columns
card = card_df.selectExpr(
    "_c1 as codcli_card",
    "_c2 as card_numcartao",
    "_c3 as card_codcon",
    "_c4 as card_tipo",
    "_c4 as flclienteouro",
    "_c5 as card_digcartao",
    "_c6 as card_status",
    "_c7 as card_dtemissao",
    "_c8 as card_dtcancelamento").filter("card_status <> 9") \
    .withColumn("flexclienteouro", when(col("card_status") == 3, \
                                        when(col("card_tipo") == 1, lit(0)) \
                                        .otherwise(0)).otherwise(lit(0))) \
    .withColumn("etl_id", lit(datetime.date.today()))

# In[16]:


destination_card = "gs://prd-lake-raw-atena/gemco_cliente_card/"
card_custom_squery = card.writeStream.partitionBy("etl_id").format("parquet").trigger(once=True).option(
    "checkpointLocation", "gs://prd-lake-transient-atena/checkpoints/crm_gemco_cliente_card/").option("path",
                                                                                                      destination_card).start().awaitTermination()

# In[17]:


# READING JUST DIFFERENCE PARTITIONS


# In[18]:


raw_customer = spark.read.parquet(destination_customer)
raw_card = spark.read.parquet(destination_card).drop("etl_id").withColumnRenamed("codcli_card", "codcli")

raw_addr = spark.read.parquet(destination_addrs).drop("etl_id")

# In[19]:


addr_residential = raw_addr.filter("tpender == 'R'")
addr_commercial = raw_addr.filter("tpender == 'M'").select(
    [(col(col_name)).alias(col_name + "_com") for col_name in raw_addr.columns]).withColumnRenamed("codcli_com",
                                                                                                   "codcli")

# In[20]:


joined_df = raw_customer.join(addr_residential, 'codcli', 'left_outer').join(addr_commercial, 'codcli',
                                                                             'left_outer').join(raw_card, 'codcli',
                                                                                                'left_outer')

# In[22]:


customer_full = joined_df.select("codcli", "natjur", "dtcadast", "tsultalt", "nomcli", "fantasia",
                                 "dtnasc", "cgccpf", "flvalidcgccpf", "numdoc", "emissor",
                                 "sexo_tratado", "estcivil", "nomepai", "nomemae", "codfilcad",
                                 "flativo", "codcargo", "vlrendbruto", "nomeconjuge", "dtultmcomp",
                                 "sitcredneg", "cpfconjuge", "flnaocontactar", "coddevcorresp", "devcorresp",
                                 "vltotcompras", "qtcompras", "maiorcmpvalor", "nrtotspc", "ultresultspc",
                                 "dtultconsspc", "cep", "codlograd", "lograd", "endereco_completo",
                                 "endereco", "numero", "complemento", "cidade", "uf", "estado", "bairro",
                                 "cep_com", "codlograd_com", "lograd_com", "endereco_completo_com", "endereco_com",
                                 "numero_com", "complemento_com", "cidade_com", "uf_com", "estado_com",
                                 "bairro_com", "e_mail", "flvalidemail", "flcorrespond", "dddfone", "fone",
                                 "dddcel", "telcel", "dddfone_com", "fone_com", "dddcel_com", "telcel_com",
                                 "card_numcartao", "flclienteouro", "flexclienteouro", "card_dtemissao",
                                 "card_dtcancelamento", "etl_id")

# In[23]:


squery = customer_full.write.partitionBy("etl_id").mode("overwrite").parquet(
    "gs://prd-lake-raw-atena/gemco_cliente_full/")
