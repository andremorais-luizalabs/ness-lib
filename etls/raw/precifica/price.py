
# coding: utf-8

# In[112]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from unicodedata import normalize, category


# In[113]:


RAW_BUCKET = "gs://prd-lake-raw-precifica/price_new/"
TRANSIENT_BUCKET = "gs://prd-lake-transient-precifica/price_new/"


# In[114]:


ss = SparkSession.builder.appName("PrecificaPriceBatchTransientToRaw").getOrCreate()


# In[115]:


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
    StructField("normal_price", DecimalType(15,2), True),
    StructField("discount_price", DecimalType(15,2), True),
    StructField("string_date", StringType(), True)
])


# In[116]:


def remove_accents(str_input):
    """
    Remove accents of the string
    """
    if not str_input:
        return None

    return ''.join((c for c in normalize('NFD', str_input) if category(c) != 'Mn'))

remove_accents_udf = udf(remove_accents)


# In[118]:


sdf = ss.readStream.option("delimiter", ";").option("header", True).csv(TRANSIENT_BUCKET, schema=schema)
sdf.createOrReplaceTempView("price_table")


# In[119]:


sdf = ss.sql("select sku, reference, name, department, category, brand, tags, status, similar, store_name, seller, normal_price, discount_price, case when length(string_date) > 16 then substring(string_date,1,16) else string_date end as string_date from price_table where string_date is not null")


# In[120]:


sdf = sdf.withColumn('last_compare_date', to_timestamp(col("string_date"), 'yyyy-MM-dd HH:mm')) .withColumn("partition_date", to_date(col("last_compare_date")))


# In[121]:


sdf = sdf.select(lower(col("sku")).alias("sku"),                  lower(remove_accents_udf(col("reference"))).alias("reference"),                  lower(remove_accents_udf(col("name"))).alias("name"),                  lower(remove_accents_udf(col("department"))).alias("department"),                  lower(remove_accents_udf(col("category"))).alias("category"),                  lower(remove_accents_udf(col("brand"))).alias("brand"),                  lower(remove_accents_udf(col("tags"))).alias("tags"),                  lower(remove_accents_udf(col("status"))).alias("status"),                  lower(col("similar")).alias("similar"),                  lower(regexp_replace(regexp_replace(remove_accents_udf(col("store_name")), '((www\.)?)(\w+)((.com.br|.com)?)','$3'), '\s','')).alias("store_name"),                  lower(regexp_replace(regexp_replace(remove_accents_udf(col("seller")), '((www\.)?)(\w+)((.com.br|.com)?)','$3'), '\s','')).alias("seller"),                  col("normal_price").alias("normal_price"),                  col("discount_price").alias("discount_price"),                  col("last_compare_date").alias("last_compare_date"),                  col("partition_date").alias("partition_date")
)


# In[122]:


sdf.writeStream.partitionBy('partition_date') .outputMode('append') .trigger(once=True) .option("path", RAW_BUCKET) .option("checkpointLocation", "gs://prd-lake-transient-precifica/checkpoints/price_new/") .start() .awaitTermination()

