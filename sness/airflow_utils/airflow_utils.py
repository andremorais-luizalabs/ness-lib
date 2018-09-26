from sness.config.cluster import *
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator
from airflow.utils.trigger_rule import TriggerRule


def DataprocClusterCreate(dag, num_workers=12, num_preemptible=0):
    return DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        project_id=PROJECT,
        cluster_name=dag.dag_id.lower()+"-cluster",
        storage_bucket=DEFAULT_CLUSTER_NAME,
        zone=ZONE,
        region=REGION,
        num_workers=num_workers,
        init_actions_uris=ACTION_SCRIPTS,
        metadata=METADATA,
        properties=PROPERTIES,
        master_machine_type=MASTER_MACHINE_TYPE,
        worker_machine_type=WORKER_MACHINE_TYPE,
        num_preemptible_workers=num_preemptible,
        dag=dag)


def DataprocClusterDelete(dag):
    return DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name=dag.dag_id.lower()+"-cluster",
        project_id=PROJECT,
        region=REGION,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE
    )
