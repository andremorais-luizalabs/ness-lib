from .config import PROJECT, ZONE, REGION, DEFAULT_CLUSTER_NAME

PROPERTIES= {
              "spark:spark.debug.maxToStringFields": "250",
              "spark:spark.driver.cores": "5",
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
ACTION_SCRIPTS = ["gs://prd-cluster-config/dataproc/jupyter.sh"]

DEFAULT_CLUSTER = {
    'region': REGION,
    'zone': ZONE,
    'project': PROJECT,
    'master_machine_type': 'n1-standard-16',
    'master_disk_type': 'pd-standard',
    'master_disk_size': 500,
    'worker_machine_type': 'n1-standard-16',
    'worker_disk_type': 'pd-standard',
    'worker_disk_size': 500,
    'num_workers': 1,
    'num_preemptible_workers': 1,
    'init_actions_uris': ["gs://prd-cluster-config/dataproc/jupyter.sh"],
    'metadata': {
        "JUPYTER_PORT": "8124",
        "JUPYTER_CONDA_PACKAGES": "numpy:pandas:scikit-learn"
    },
    'properties': {
        PROPERTIES
    },
    'storage_bucket':'prd-lake-dataproc-metainfo'
}
