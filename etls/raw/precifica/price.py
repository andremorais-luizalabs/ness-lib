
# coding: utf-8

# In[10]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from unicodedata import normalize, category


# In[11]:


RAW_BUCKET = "gs://prd-lake-raw-precifica/price_new/"
TRANSIENT_BUCKET = "gs://prd-lake-transient-precifica/price_new/"


# In[12]:


ss = SparkSession.builder.appName("PrecificaPriceBatchTransientToRaw").getOrCreate()


# In[13]:


schema = StructType([
    StructField("sku", LongType(), True),
    StructField("reference", StringType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("status", StringType(), True),
    StructField("similar", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("seller", StringType(), True),
    StructField("normal_price", DecimalType(10, 2), True),
    StructField("discount_price", DecimalType(10, 2), True),
    StructField("String Date", StringType(), True)
])


#SKU;Código Referência;Nome;Departamento;Categoria/Setor;Marca;Tags;Status;Similar;Loja;Vendido Por;Preço Normal;Preço Oferta;Data Ultima Comparacao;


# In[14]:


def remove_accents(str_input):
    """
    Remove accents of the string
    """
    if not str_input:
        return ''
    
    return ''.join((c for c in normalize('NFD', str_input) if category(c) != 'Mn'))


remove_accents_udf = udf(remove_accents)


# In[15]:


sdf = ss.readStream.option("delimiter", ";").option("header", True).csv(TRANSIENT_BUCKET, schema=schema)


# In[16]:


sdf = sdf.withColumn('last_compare_date', to_timestamp(col("String Date"), 'yyyy-MM-dd HH:mm')) .withColumn("partition_date", to_date(col("last_compare_date")))

sdf = sdf.drop("String Date")

sdf = sdf.select(lower(col("sku")).alias("sku"),         lower(remove_accents_udf(col("reference"))).alias("reference"),         lower(remove_accents_udf(col("name"))).alias("name"),         lower(remove_accents_udf(col("department"))).alias("department"),         lower(remove_accents_udf(col("category"))).alias("category"),         lower(remove_accents_udf(col("brand"))).alias("brand"),         lower(remove_accents_udf(col("tags"))).alias("tags"),         lower(remove_accents_udf(col("status"))).alias("status"),         lower(col("similar")).alias("similar"),         lower(regexp_replace(regexp_replace(remove_accents_udf(col("store_name")), '((www\.)?)(\w+)((.com.br|.com)?)','$3'), '\s','')).alias("store_name"),         lower(regexp_replace(regexp_replace(remove_accents_udf(col("seller")), '((www\.)?)(\w+)((.com.br|.com)?)','$3'), '\s','')).alias("seller"),         col("normal_price").alias("normal_price"),         col("discount_price").alias("discount_price"),         col("last_compare_date").alias("last_compare_date"),
        col("partition_date").alias("partition_date")
)


# In[17]:


sdf.writeStream.partitionBy('partition_date') .outputMode('append') .trigger(once=True) .option("path", RAW_BUCKET) .option("checkpointLocation", "gs://prd-lake-transient-precifica/checkpoints/price_new/") .start() .awaitTermination()

