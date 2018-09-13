from ..config.cluster import *
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator

def DataprocClusterCrea(dag):
    return DataprocClusterCreateOperator(
            task_id='create_dataproc_cluster',
            project_id=PROJECT,
            cluster_name=DEFAULT_CLUSTER_NAME,
            zone=ZONE,
            region=REGION,
            num_workers=2,
            init_actions_uris=,
            metadata=METADATA,
            master_machine_type=MASTER_MACHINE_TYPE,
            worker_machine_type=WORKER_MACHINE_TYPE,
            num_preemptible_workers=10,
            graceful_decommission_timeout='1h',
            dag=dag)