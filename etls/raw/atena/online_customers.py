# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from unicodedata import normalize, category
from functools import reduce
import datetime

# In[2]:


spark = SparkSession.builder.appName("RAW_ATENA_ONLINE_CUSTOMER").enableHiveSupport().getOrCreate()

# In[3]:


import re

RE_CPF = re.compile(r'(?:\d{11})')
RE_CNPJ = re.compile(r'(?:\d{14})')


def remove_special_characters(str_input, excl_chars):
    """
    Remove special characters entered of the string
    - str_input: String to clear special characters
    - excl_chars: String of the characters to delete
    """
    if str_input is None:
        return ''

    list_chars = list(excl_chars)

    for i in range(0, len(excl_chars)):
        str_input = str_input.replace(list_chars[i], ' ')

    str_input = str_input.replace('\\', ' ').replace('\x09', ' ').replace('\x5C', ' ').replace('  ', ' ').strip()
    return str_input


def valid_syntax_email(str_email):
    """
    Valide email.
    - Parameter: string type
    - Return (int type): 1-Ok and 0-Nok
    """
    match = re.match(r'^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+'
                     r'(\.[a-z0-9-]+)*(\.[a-z]{2,4})$', str_email)

    if match:
        return 1
    else:
        return 0


def remove_accents(str_input):
    """
    Remove accents of the string
    """
    if not str_input:
        return ''

    return ''.join((c for c in normalize('NFD', str_input) if category(c) != 'Mn'))


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


def get_ddd_from_fullphone(str_phone):
    """
    Capture the DDD of a full phone number
    - Parameter: string type
    - Return: string type
    """
    try:
        str_ph = str(int(''.join(re.findall('\d', str(str_phone)))))

        if len(str_ph) in (10, 11):
            return str_ph[:2]
        if len(str_ph) in (12, 13):
            return str_ph[2:4]
        else:
            return ''
    except ValueError:
        return ''


get_ddd_from_fullphone = udf(get_ddd_from_fullphone)

estado_civil = {('NOIVO' 'SOLTEIRO'): 'SOLTEIRO',
                'CASADO': 'CASADO',
                'SEPARADO': 'SEPARADO',
                ('VI�VO', 'VIUVO'): 'VIUVO',
                'DIVORCIADO': 'DIVORCIADO',
                'UNIAO ESTAV': 'UNIAO ESTAVEL'}

valid_cgccpf = udf(valid_cgccpf)
remove_special = udf(remove_special_characters)
remove_accents_udf = udf(remove_accents)
estado_civil_udf = udf(lambda x: estado_civil.get(x, None) if x else None)
valid_email_udf = udf(valid_syntax_email)

# In[4]:


schema = spark.read.option("delimiter", "|").csv("gs://prd-lake-transient-atena/atena/online_cliente/").schema

df = spark.readStream.option("delimiter", "|").csv("gs://prd-lake-transient-atena/atena/online_cliente/", schema=schema)

# In[5]:


original_columns = df.columns
new_columns = ["id_cliente", "email_cliente", "cep_cliente", "ind_pessoa_fisica_cliente",
               "id_tabloja", "cpf_cnpj_cliente", "dt_cadastro_cliente", "dt_altcadastro_cliente",
               "boletin_email", "logonoff_cliente", "optin_celular", "blnemail_valido",
               "uuid", "bairro_cliente_pf", "uff", "cidade_cliente_pf", "estado",
               "sexo_cliente_pf", "dt_nascimento_cliente_pf", "estado_civil", "hour",
               "name", "first_name", "celphone", "campanha", "simplecustomeridlookup",
               "cadastredayidlookup", "ramo_atividade", "ramooutro_cliente_pj", "numero_funcionario",
               "cpf_responsavel_cliente_pj", "endereco_cliente", "numero_end_cliente",
               "complemento_end_cliente", "homephone"]

list_transform = ["lower(email_cliente) as email_cliente",
                  "cast(ind_pessoa_fisica_cliente as boolean) as ind_pessoa_fisica_cliente",
                  "cast(boletin_email as boolean) as boletin_email",
                  "cast(logonoff_cliente as boolean) as logonoff_cliente",
                  "cast(optin_celular as boolean) as optin_celular",
                  "lower(uuid) as uuid",
                  "upper(bairro_cliente_pf) as bairro_cliente_pf",
                  "upper(uff) as uff",
                  "upper(cidade_cliente_pf) as cidade_cliente_pf",
                  "upper(estado) as estado",
                  "upper(name) as name",
                  "upper(estado_civil) as estado_civil",
                  "upper(first_name) as first_name",
                  "date_format(dt_cadastro_cliente, 'yyyy-mm-dd') as dt_cadastro_cliente",
                  "date_format(dt_nascimento_cliente_pf, 'yyyy-mm-dd') as dt_nascimento_cliente_pf",
                  "date_format(dt_altcadastro_cliente, 'yyyy-mm-dd HH:mm:ss') as dt_altcadastro_cliente",
                  "upper(endereco_cliente) as endereco_cliente"]

select_expr_list = [x for x in new_columns if x not in [x for y in list_transform if x in y]]

trimmed_df = df.select([remove_accents_udf(col(col_name)).alias(col_name) for col_name in df.columns]).select(
    [trim(col(col_name)).alias(col_name) for col_name in df.columns]).select(
    [col(x).alias(new_columns[y]) for y, x in enumerate(original_columns)]).selectExpr(
    select_expr_list + list_transform).filter(col("id_cliente").isNotNull())

# In[13]:


transform = trimmed_df.withColumn("flvalidemail", valid_email_udf(col("email_cliente"))).withColumn("natureza_juridica",
                                                                                                    when(col(
                                                                                                        "ind_pessoa_fisica_cliente") == False,
                                                                                                         lit(
                                                                                                             "J")).otherwise(
                                                                                                        lit(
                                                                                                            "F"))).withColumn(
    "dt_cadastro_cliente", date_format("dt_cadastro_cliente", "yyyy-mm-dd")).withColumn("estado_civil",
                                                                                        estado_civil_udf(col(
                                                                                            "estado_civil"))).withColumn(
    "origem", lit("ECOMM")).withColumn("valid_cgccpf",
                                       valid_cgccpf("natureza_juridica", "cpf_cnpj_cliente")).withColumn("dddcel",
                                                                                                         get_ddd_from_fullphone(
                                                                                                             "celphone")).withColumn(
    "dddphone", get_ddd_from_fullphone("celphone")).withColumn("codlograd_cliente", lit("NAO INFORMADO")).withColumn(
    "lograd_cliente", lit("NAO INFORMADO")).withColumn("endereco_completo", concat(col("endereco_cliente"), lit(" "),
                                                                                   col("numero_end_cliente"), lit(" "),
                                                                                   col("complemento_end_cliente")))

transform = transform.select(new_columns + ["flvalidemail", "natureza_juridica",
                                            "origem", "valid_cgccpf", "dddcel",
                                            "dddphone", "codlograd_cliente",
                                            "lograd_cliente", "endereco_completo"]) \
    .withColumn("etl_id", lit(datetime.date.today()))

# In[ ]:


squery = transform.writeStream.partitionBy("etl_id").format("parquet").trigger(once=True).option("checkpointLocation",
                                                                                                 "gs://prd-lake-transient-atena/checkpoints/crm_online_cliente/").option(
    "path", "gs://prd-lake-raw-atena/online_cliente/").start().awaitTermination()
