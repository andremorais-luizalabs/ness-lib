from ..config.cluster import *
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator

def DataprocClusterCreate(dag):
    return DataprocClusterCreateOperator(
            task_id='create_dataproc_cluster',
            project_id=PROJECT,
            cluster_name=DEFAULT_CLUSTER_NAME,
            storage_bucket=DEFAULT_CLUSTER_NAME,
            zone=ZONE,
            region=REGION,
            num_workers=2,
            init_actions_uris=ACTION_SCRIPTS,
            metadata=METADATA,
            properties=PROPERTIES,
            master_machine_type=MASTER_MACHINE_TYPE,
            worker_machine_type=WORKER_MACHINE_TYPE,
            num_preemptible_workers=10,
            graceful_decommission_timeout='1h',
            dag=dag)

def DataprocClusterDelete(dag):
    return DataprocClusterDeleteOperator(
        cluster_name=DEFAULT_CLUSTER_NAME,
        project_id=PROJECT,
        region=REGION,
        dag=dag
    )