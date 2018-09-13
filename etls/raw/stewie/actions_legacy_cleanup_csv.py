
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


# In[2]:


RAW_BUCKET = "gs://prd-lake-raw-stewie/actions/"
TRANSIENT_BUCKET = "gs://prd-lake-transient-stewie/migration/stewie/actions/"


# In[3]:


ss = SparkSession.builder.appName("ActionCleanUp").enableHiveSupport().getOrCreate()


# In[4]:


def replace_n_to_null(text):
    if text == "\\N":
        return None
    else:
        return text


# In[5]:


schema = ss.read.option("delimiter", "|").csv(TRANSIENT_BUCKET).schema
sdf = ss.readStream.option("delimiter", "|").csv(TRANSIENT_BUCKET, schema=schema)

udf_replace_n = udf(replace_n_to_null)

named_sdf_non_null = sdf.select([udf_replace_n(col(coluna)).alias(coluna) for coluna in sdf.columns])

final_sdf = named_sdf_non_null.selectExpr("_c0 as action",
                                          "cast(_c1/1000 as timestamp) as action_timestamp",
                                          "cast(_c2 as integer) as quantity",
                                          "_c3 as product_id",
                                          "_c4 as product_name",
                                          "cast(_c5 as float) as price",
                                          "cast(_c6 as float) as interaction_price",
                                          "_c7 as category_id",
                                          "_c8 as category_name",
                                          "_c9 as department_id",
                                          "_c10 as department_name",
                                          "_c11 as brand_id",
                                          "_c12 as brand_name",
                                          "cast(_c13 as float) as review_score",
                                          "_c14 as person_uuid",
                                          "_c15 as person_id",
                                          "_c16 as person_temp_id",
                                          "_c17 as person_name",
                                          "_c18 as email",
                                          "_c19 as cpf",
                                          "_c20 as city",
                                          "_c21 as state",
                                          "_c22 as village",
                                          "_c23 as gender",
                                          "_c24 as zip_code",
                                          "_c25 as birth_date",
                                          "_c26 as channel_id",
                                          "_c27 as session_id",
                                          "cast(_c28 as boolean) as identified_session",
                                          "_c29 as user_name",
                                          "_c30 as additional_info",
                                          "cast(_c31 as date) as day_interacted_at"
                                         )

final_sdf.writeStream .partitionBy('day_interacted_at') .outputMode('append') .trigger(once=True) .option("path", "gs://prd-lake-raw-stewie/actions/") .option("checkpointLocation", "gs://prd-lake-transient-stewie/checkpoints/migration/actions/") .start() .awaitTermination()

