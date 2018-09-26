# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from unicodedata import normalize, category
from pyjarowinkler import distance
from datetime import datetime, date
from pyjarowinkler.distance import get_jaro_distance
from sness.etl import dataframe2trusted

# In[2]:


spark = SparkSession.builder.appName("TRUSTED_ATENA_ONLINE_CUSTOMER").enableHiveSupport().getOrCreate()

# In[3]:


# -*- coding: utf-8 -*-

MIN_INDEX = 0
MAX_INDEX = 5

WEIGHTS = {
    'LUIZACRED': [56, 55, 54, 53, 52, 51],
    'ONLINE': [55, 54, 53, 52, 51, 50],
    'GEMCO': [54, 53, 52, 51, 50, 49],
}


def get_base_weight(base, basedate):
    if not basedate:
        return WEIGHTS[base][5]
    now = date.today()
    delta = (now - basedate).days / 30.0
    delta = int(delta)
    index = delta if delta >= MIN_INDEX and delta <= MAX_INDEX else MAX_INDEX

    return WEIGHTS[base][index]


get_base_weight = udf(get_base_weight)


def get_tpvalidation(tprecord, origin_number):
    if not tprecord:
        return 0
    elif tprecord == 1:
        return 1
    elif origin_number and origin_number != 'NAO INFORMADO' and tprecord == 2:
        return 2
    elif (not origin_number or origin_number == 'NAO INFORMADO') and tprecord == 2:
        return 3


get_tpvalidation = udf(get_tpvalidation)

STATES = {'AC': 'ACRE', 'AL': 'ALAGOAS', 'AP': 'AMAPA', 'AM': 'AMAZONAS',
          'BA': 'BAHIA', 'CE': 'CEARA', 'DF': 'DISTRITO FEDERAL',
          'ES': 'ESPIRITO SANTO', 'GO': 'GOIAS', 'MA': 'MARANHAO',
          'MT': 'MATO GROSSO', 'MS': 'MATO GROSSO DO SUL',
          'MG': 'MINAS GERAIS', 'PA': 'PARA', 'PB': 'PARAIBA',
          'PR': 'PARANA', 'PE': 'PERNAMBUCO', 'PI': 'PIAUI',
          'RJ': 'RIO DE JANEIRO', 'RN': 'RIO GRANDE DO NORTE',
          'RS': 'RIO GRANDE DO SUL', 'RO': 'RONDONIA', 'RR': 'RORAIMA',
          'SC': 'SANTA CATARINA', 'SP': 'SAO PAULO', 'SE': 'SERGIPE',
          'TO': 'TOCANTINS'}

get_state = udf(lambda x: STATES.get(x, ''))


def is_the_same_customer(jarowd, fjarowd, bday_s=None, bday_t=None,
                         name_s=None, name_t=None):
    """
    Compares the source customer to the single customer and tells if they
    are the same. It uses the jaro winkler distance to check this information
    - bday_s: Data nasc origem
    - bday_t: Data nasc destino
    - fname_s: Primeiro nome origem
    - fname_t: Primeiro nome destino
    - jaro_distance: distance full
    - fjaro_distance: distance primeiro nome
    - lname_s: ultimo nome origem
    - lname_t: ultimo nome destino
    """
    jaro_distance = 1.0 - jarowd
    fjaro_distance = 1.0 - fjarowd

    # Ajusts of values in the fisrt name and target name
    name_s = '' if name_s is None else name_s
    name_t = '' if name_t is None else name_t

    name_s = unicode(name_s) if not isinstance(name_s, unicode) else name_s
    name_t = unicode(name_t) if not isinstance(name_t, unicode) else name_t

    # Get fisrt name from source and target names
    fname_s = tuple(name_s.split(' '))[0]
    fname_t = tuple(name_t.split(' '))[0]

    # Get last name from source and target names
    lname_s = tuple(name_s.split(' '))[len(tuple(name_s.split(' '))) - 1]
    lname_t = tuple(name_t.split(' '))[len(tuple(name_t.split(' '))) - 1]

    # CPF e data de nascimento iguais
    if (bday_s and bday_s == bday_t):
        # Abrange mudança de nome civil
        if (fname_s and fname_s == fname_t):
            return 1
        # Abrange similaridade de nome e abreviação de nomes
        elif (jaro_distance < 0.42):
            return 1
        # Erro de digitação do primeiro nome
        elif (lname_s and lname_s == lname_t) and fjaro_distance < 0.21:
            return 1
        # Abrange nomes nulos
        elif (not name_s or not name_t):
            return 1
        else:
            return 0
    # CPF igual, nome similare e data de nascimento errada */
    else:
        # Primeiro Nome igual e data nascimento errada
        if (fname_s and fname_s == fname_t) and jaro_distance < 0.21:
            return 1
        # Erro de digitação do primeiro nome
        elif (lname_s and lname_s == lname_t) and fjaro_distance < 0.14:
            return 1
        else:
            return 0


is_the_same_customer = udf(is_the_same_customer)
jaro_distance = udf(get_jaro_distance)


# In[4]:


def __orderDFAndAddMissingCols(df, columnsOrderList, dfMissingFields):
    ''' return ordered dataFrame by the columns order list with null in missing columns '''
    if not dfMissingFields:  # no missing fields for the df
        return df.select(columnsOrderList)
    else:
        columns = []
        for colName in columnsOrderList:
            if colName not in dfMissingFields:
                columns.append(colName)
            else:
                columns.append(lit(None).alias(colName))
        return df.select(columns)


def __addMissingColumns(df, missingColumnNames):
    ''' Add missing columns as null in the end of the columns list '''
    listMissingColumns = []
    for col in missingColumnNames:
        listMissingColumns.append(lit(None).alias(col))

    return df.select(df.schema.names + listMissingColumns)


def __orderAndUnionDFs(leftDF, rightDF, leftListMissCols, rightListMissCols):
    ''' return union of data frames with ordered columns by leftDF. '''
    leftDfAllCols = __addMissingColumns(leftDF, leftListMissCols)
    rightDfAllCols = __orderDFAndAddMissingCols(rightDF, leftDfAllCols.schema.names, rightListMissCols)
    return leftDfAllCols.union(rightDfAllCols)


def unionDFs(leftDF, rightDF):
    ''' Union between two dataFrames, if there is a gap of column fields,
     it will append all missing columns as nulls '''
    # Check for None input
    if leftDF == None:
        raise ValueError('leftDF parameter should not be None')
    if rightDF == None:
        raise ValueError('rightDF parameter should not be None')
    else:  # Different columns
        # Save dataFrame columns name list as set
        leftDFColList = set(leftDF.schema.names)
        rightDFColList = set(rightDF.schema.names)
        # Diff columns between leftDF and rightDF
        rightListMissCols = list(leftDFColList - rightDFColList)
        leftListMissCols = list(rightDFColList - leftDFColList)
        return __orderAndUnionDFs(leftDF, rightDF, leftListMissCols, rightListMissCols)


# In[5]:


# READING CEP
zipc = spark.read.parquet("gs://prd-lake-raw-atena/zipcode")

# In[6]:


original_gemco = ["codcli", "natjur", "dtcadast", "tsultalt", "nomcli", "fantasia",
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
                  "card_dtcancelamento", "etl_id"]
renamed_gemco = ["id_gemco", "tipopessoa", "dtcad",
                 "dtaltcad", "nome", "fantasia", "dtnasc",
                 "cpf_cnpj", "flvalida_cpf_cnpj", "numdoc", "emissor",
                 "sexo", "estcivil", "nomepai", "nomemae",
                 "codfilialcad", "flativo_gemco", "codcargo", "vlrenda",
                 "nomeconjuge", "dtultmcomp", "sitcredneg", "cpfconjuge",
                 "flnaocontactar", "coddevcorresp", "devcorresp",
                 "vltotcompras", "qtcompras", "vlmaiorcompra",
                 "nrtotspc", "ultresultspc", "dtultconsspc", "cep_res",
                 "codlograd_res", "lograd_res", "endereco_completo",
                 "endereco_res", "numero_res", "complemento_res", "cidade_res",
                 "uf_res", "estado_res", "bairro_res", "cep_com",
                 "codlograd_com", "lograd_com", "endereco_completo_com",
                 "endereco_com", "numero_com", "complemento_com", "cidade_com",
                 "uf_com", "estado_com", "bairro_com", "email",
                 "flvalidemail", "flcorrespond", "dddfone", "numfone",
                 "dddcel", "numcel", "dddfone_com", "numfone_com",
                 "dddcel_com", "numcel_com", "numcartao", "flclienteouro",
                 "flexclienteouro", "dtemissaocartao", "dtcanccartao", "etl_id"]

ranking = get_base_weight(lit("GEMCO"), to_date(coalesce("dtaltcad", "dtcad")))

# READING GEMCO CUSTOMER
gemco_customer = spark.read.parquet("gs://prd-lake-raw-atena/gemco_cliente_full/").select(
    [col(x).alias(renamed_gemco[original_gemco.index(x)]) for x in original_gemco]).withColumn("peso_base_origem",
                                                                                               ranking)

# In[7]:


# READING ONLINE CUSTOMER

original_online = ["id_cliente", "email_cliente", "cep_cliente", "natureza_juridica", "id_tabloja",
                   "cpf_cnpj_cliente", "valid_cgccpf", "dt_cadastro_cliente",
                   "dt_altcadastro_cliente", "boletin_email", "logonoff_cliente", "optin_celular",
                   "blnemail_valido", "uuid", "bairro_cliente_pf", "uff", "cidade_cliente_pf",
                   "estado", "sexo_cliente_pf", "dt_nascimento_cliente_pf", "estado_civil", "name",
                   "first_name", "dddcel", "celphone", "dddphone", "homephone", "campanha",
                   "ramo_atividade", "ramooutro_cliente_pj", "numero_funcionario",
                   "cpf_responsavel_cliente_pj", "endereco_completo", "endereco_cliente",
                   "numero_end_cliente", "complemento_end_cliente", "codlograd_cliente",
                   "lograd_cliente"]

new_online = ["id_site", "email_site", "cep_res",
              "tipopessoa", "origemsite_idloja", "cpf_cnpj",
              "flvalida_cpf_cnpj", "dtcad_online", "dtaltcad_online",
              "optin_email_site", "flativo_site", "optin_celular_site",
              "blnemail_valido", "uuid_site", "bairro_res",
              "uf_res", "cidade_res", "estado_res", "sexo",
              "dtnasc", "estcivil", "nome", "fantasia",
              "dddcel", "numcel", "dddfone", "numfone",
              "origemsite_campanha", "ramo_atividade",
              "ramooutro_cliente_pj", "numero_funcionario",
              "cpfresponsavelpj", "endereco_completo", "endereco_res",
              "numero_res", "complemento_res", "codlograd_res",
              "lograd_res"]

ranking = get_base_weight(lit("ONLINE"), to_date(coalesce("dtcad_online", "dtaltcad_online")))

# READING ONLINE
online_customer = spark.read.parquet("gs://prd-lake-raw-atena/online_cliente/").select(
    [col(x).alias(new_online[original_online.index(x)]) for x in original_online]).withColumn("peso_base", ranking)

# In[8]:


single_customer_cols = ["id_gemco", "id_site", "uuid_site", "tipopessoa", "dtcad",
                        "dtaltcad", "dtcad_online", "dtaltcad_online", "nome", "fantasia", "dtnasc",
                        "cpf_cnpj", "numdoc", "emissor", "sexo", "estcivil", "nomepai", "nomemae",
                        "codfilialcad", "origemsite_idloja", "flativo_gemco", "codcargo", "vlrenda",
                        "nomeconjuge", "sitcredneg", "cpfconjuge", "flnaocontactar", "coddevcorresp",
                        "devcorresp", "nrtotspc", "ultresultspc", "dtultconsspc", "cep_res",
                        "codlograd_res", "lograd_res", "endereco_completo", "endereco_res",
                        "numero_res", "complemento_res", "cidade_res", "uf_res", "estado_res",
                        "bairro_res", "email", "email_site", "flvalidemail", "blnemail_valido",
                        "optin_email_site", "flativo_site", "optin_celular_site", "flcorrespond",
                        "dddfone", "numfone", "dddcel", "numcel", "dddfone_com", "numfone_com",
                        "dddcel_com", "numcel_com", "numcartao", "flclienteouro", "flexclienteouro",
                        "dtemissaocartao", "dtcanccartao", "flvalidcep", "cep_validation",
                        "cep_tratado", "codlograd_tratado", "lograd_tratado", "endereco_compl_tratado",
                        "endereco_tratado", "numero_tratado", "complemento_tratado", "bairro_tratado",
                        "cidade_tratado", "uf_tratado", "estado_tratado", "peso_base",
                        "origemsite_campanha", "ramo_atividade", "ramooutro_cliente_pj",
                        "numero_funcionario", "cpfresponsavelpj"]

# In[ ]:


# online_customer.


# In[9]:


unionDF = unionDFs(online_customer, gemco_customer)
grouped = unionDF.groupBy("cpf_cnpj").agg(*[last(x, True).alias(x) for x in unionDF.columns if x != "cpf_cnpj"])

cond = [grouped.cep_res == zipc.cep, grouped.cep_com == zipc.cep]

final = grouped.join(zipc, cond, "left_outer").drop("cep", "etl_id").withColumn("cep_validation",
                                                                                get_tpvalidation("tipo_cep",
                                                                                                 "numero_res")).withColumn(
    "flvalidcep", when(col("cep_validation") != 0, 1).otherwise(lit(0))).withColumn("endereco_compl_tratado",
                                                                                    concat(col("codlograd_cep"),
                                                                                           lit(" "),
                                                                                           col("numero_res"), lit(" "),
                                                                                           col("complemento_res"))) \
    .withColumn("cep_tratado", col("cep_res")) \
    .withColumn("codlograd_tratado",
                when(col("cep_validation") == 2, col("codlograd_cep")).otherwise(col("codlograd_res"))) \
    .withColumn("lograd_tratado", when(col("cep_validation") == 2, col("lograd_cep")).otherwise(col("lograd_res"))) \
    .withColumn("endereco_compl_tratado",
                when(col("cep_validation") == 2, col("endereco_compl_tratado")).otherwise(col("endereco_completo"))) \
    .withColumn("endereco_tratado",
                when(col("cep_validation") == 2, col("endereco_cep")).otherwise(col("endereco_res"))) \
    .withColumn("bairro_tratado", when(col("cep_validation") == 2, col("bairro_cep")).otherwise(col("bairro_res"))) \
    .withColumn("cidade_tratado", when(col("cep_validation") == 0, col("cidade_res")).otherwise(col("cidade_cep"))) \
    .withColumn("uf_tratado", when(col("cep_validation") == 0, col("uf_res")).otherwise(col("uf_cep"))) \
    .withColumn("estado_tratado",
                when(col("cep_validation") == 0, col("estado_res")).otherwise(get_state(col("uf_cep")))) \
    .withColumn("numero_tratado", col("numero_res")) \
    .withColumn("complemento_tratado", col("complemento_res")) \
    .select(single_customer_cols) \
    .withColumn("alter_date", date_format("dtaltcad", "yyyy-MM-dd")) \
    .withColumn("etl_id", lit(date.today()))



# In[12]:
dataframe2trusted(final, namespace="atena", dataset="single_customer", partition_by=["alter_date"])
# final.repartition("alter_date").write.mode("overwrite").parquet("gs://prd-lake-trusted-atena/single_customer/")
