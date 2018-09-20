# In[1]:


from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import time
from datetime import datetime
import json


# In[2]:


RAW_BUCKET = "gs://prd-lake-raw-stewie/actions/"
TRANSIENT_BUCKET = "gs://prd-lake-transient-stewie/actions/"


# In[3]:


ss = SparkSession.builder.appName("ActionStreamTransientToRaw").enableHiveSupport().getOrCreate()


# In[4]:


schema = json.loads('{"type":"struct","fields":[{"name":"action","type":"string","nullable":true,"metadata":{}},{"name":"additionalInfo","type":"string","nullable":true,"metadata":{}},{"name":"userName","type":"string","nullable":true,"metadata":{}},{"name":"actionTimestamp","type":"long","nullable":true,"metadata":{}},{"name":"channel","type":"string","nullable":true,"metadata":{}},{"name":"interactionPrice","type":"double","nullable":true,"metadata":{}},{"name":"person","type":{"type":"struct","fields":[{"name":"birthDate","type":"string","nullable":true,"metadata":{}},{"name":"city","type":"string","nullable":true,"metadata":{}},{"name":"cpf","type":"string","nullable":true,"metadata":{}},{"name":"crId","type":"string","nullable":true,"metadata":{}},{"name":"creationTime","type":"long","nullable":true,"metadata":{}},{"name":"disabled","type":"boolean","nullable":true,"metadata":{}},{"name":"email","type":"string","nullable":true,"metadata":{}},{"name":"gender","type":"string","nullable":true,"metadata":{}},{"name":"id","type":"string","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"optIn","type":"boolean","nullable":true,"metadata":{}},{"name":"state","type":"string","nullable":true,"metadata":{}},{"name":"stewieOptIn","type":"boolean","nullable":true,"metadata":{}},{"name":"tempId","type":"string","nullable":true,"metadata":{}},{"name":"uuid","type":"string","nullable":true,"metadata":{}},{"name":"village","type":"string","nullable":true,"metadata":{}},{"name":"zipCode","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"product","type":{"type":"struct","fields":[{"name":"brand","type":{"type":"struct","fields":[{"name":"brandId","type":"string","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"cashPrice","type":"double","nullable":true,"metadata":{}},{"name":"category","type":{"type":"struct","fields":[{"name":"categoryId","type":"string","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"department","type":{"type":"struct","fields":[{"name":"departmentId","type":"string","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"inStock","type":"long","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"price","type":"double","nullable":true,"metadata":{}},{"name":"productId","type":"string","nullable":true,"metadata":{}},{"name":"quantity","type":"long","nullable":true,"metadata":{}},{"name":"reviewCount","type":"long","nullable":true,"metadata":{}},{"name":"reviewScore","type":"double","nullable":true,"metadata":{}},{"name":"stemName","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"quantity","type":"long","nullable":true,"metadata":{}},{"name":"sessionId","type":"string","nullable":true,"metadata":{}},{"name":"sessionIdentified","type":"boolean","nullable":true,"metadata":{}}]}')


# In[5]:


sdf = ss.readStream.json(TRANSIENT_BUCKET, schema=StructType.fromJson(schema))


# In[6]:


def parse_day_interacted_at(str_time):
    if not str_time:
        return None
    return datetime.utcfromtimestamp(str_time/1000).strftime('%Y-%m-%d')
parse_day_interacted_at = udf(parse_day_interacted_at)


# In[7]:


df = (
        sdf.withColumn("day_interacted_at", parse_day_interacted_at(col("actionTimestamp")))\
        .select(
            col("product.category.categoryId").alias("category_id"),
            col("product.category.name").alias("category_name"),
            col("product.department.departmentId").alias("department_id"),
            col("product.department.name").alias("department_name"),
            col("product.brand.brandId").alias("brand_id"),
            col("product.brand.name").alias("brand_name"),
            col("product.price").alias("price"),
            col("product.quantity").alias("quantity"),
            col("product.productId").alias("product_id"),
            col("product.name").alias("product_name"),
            col("product.reviewScore").alias("review_score"),
            col("person.birthDate").alias("birth_date"),
            col("person.city").alias("city"),
            col("person.cpf").alias("cpf"),
            col("person.email").alias("email"),
            col("person.gender").alias("gender"),
            col("person.id").alias("person_id"),
            col("person.name").alias("person_name"),
            col("person.state").alias("state"),
            col("person.village").alias("village"),
            col("person.tempId").alias("person_temp_id"),
            col("person.uuid").alias("person_uuid"),
            col("person.zipCode").alias("zip_code"),
            col("action").alias("action"),
            col("actionTimestamp").alias("action_timestamp"),
            col("channel").alias("channel_id"),
            col("sessionId").alias("session_id"),
            col("sessionIdentified").alias("identified_session"),
            col("interactionPrice").alias("interaction_price"),
            col("userName").alias("user_name"),
            col("additionalInfo").alias("additional_info"),
            col("day_interacted_at"))
)


# In[8]:


final_df = df.distinct().selectExpr("action", "cast(action_timestamp/1000 as timestamp) as action_timestamp", "quantity", "product_id",                    "product_name", "price", "interaction_price", "category_id",                     "category_name", "department_id", "department_name",                     "brand_id", "brand_name", "review_score", "person_uuid",                    "person_id", "person_temp_id", "person_name", "email",                    "cpf", "city", "state", "village", "gender", "zip_code",                     "birth_date", "channel_id", "session_id", "identified_session",                     "user_name", "additional_info", "day_interacted_at")


# In[9]:


final_df.writeStream.partitionBy('day_interacted_at') .outputMode('append') .trigger(once=True) .option("path", RAW_BUCKET) .option("checkpointLocation", "gs://prd-lake-transient-stewie/checkpoints/actions_stream/") .start() .awaitTermination()