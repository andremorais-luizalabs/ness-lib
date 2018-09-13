from .config import PROJECT, ZONE, REGION, DEFAULT_CLUSTER_NAME

PROPERTIES= {
              "spark:spark.debug.maxToStringFields": "250",
              "spark:spark.default.parallelism": "290",
              "spark:spark.driver.cores": "5",
              "spark:spark.driver.memory": "17G",
              "spark:spark.executor.cores": "5",
              "spark:spark.executor.instances": "29",
              "spark:spark.executor.memory": "17G",
              "spark:spark.executorEnv.PYTHONHASHSEED": "0",
              "spark:spark.yarn.am.memory": "2048m",
              "spark:spark.yarn.driver.memoryOverhead": "2048",
              "spark:spark.yarn.executor.memoryOverhead": "2048"
             }

METADATA = {
              "JUPYTER_PORT": "8124",
              "JUPYTER_CONDA_PACKAGES": "numpy:pandas:scikit-learn"
           }

MASTER_MACHINE_TYPE="n1-standard-16"
WORKER_MACHINE_TYPE="n1-standard-16"