
# coding: utf-8

# In[20]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
import json

# In[21]:


RAW_BUCKET = "gs://prd-lake-raw-atena/online_pedido/"
TRANSIENT_BUCKET = "gs://prd-lake-transient-atena/atena/online_pedido/"

# In[22]:


ss = SparkSession.builder.appName("AtenaOrdersBatchTransientToRaw").enableHiveSupport().getOrCreate()

# In[23]:


schema = json.loads('{"type":"struct","fields":[{"name":"clientid","type":"integer","nullable":true,"metadata":{}},{"name":"orderidnaturalkey","type":"integer","nullable":true,"metadata":{}},{"name":"sourcechannel","type":"string","nullable":true,"metadata":{}},{"name":"partnerid","type":"integer","nullable":true,"metadata":{}},{"name":"core","type":"string","nullable":true,"metadata":{}},{"name":"subcore","type":"string","nullable":true,"metadata":{}},{"name":"captureday","type":"date","nullable":true,"metadata":{}}]}')

# In[24]:


sdf = ss.readStream.option("delimiter", "|").option("header", "true").csv(TRANSIENT_BUCKET, schema=StructType.fromJson(schema))

# In[25]:


sdf.writeStream.partitionBy('captureday') \
.outputMode('append') \
.trigger(once=True) \
.option("path", RAW_BUCKET) \
.option("checkpointLocation", "gs://prd-lake-transient-atena/checkpoints/online_pedido/") \
.start() \
.awaitTermination()
