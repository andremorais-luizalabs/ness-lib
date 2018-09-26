from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator
from airflow.utils.trigger_rule import TriggerRule
from sness.config.cluster import DEFAULT_CLUSTER

"""
Create a new cluster on Google Cloud Dataproc. The operator will wait until the
creation is successful or an error occurs in the creation process.
The parameters allow to configure the cluster. Please refer to
https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters
for a detailed explanation on the different parameters. Most of the configuration
parameters detailed in the link are available as a parameter to this operator.
:param num_workers: The # of workers to spin up
:type num_workers: int
:param init_action_timeout: Amount of time executable scripts in
    init_actions_uris has to complete
:type init_action_timeout: str
:param image_version: the version of software inside the Dataproc cluster
:type image_version: str
:param custom_image: custom Dataproc image for more info see
    https://cloud.google.com/dataproc/docs/guides/dataproc-images
:type: custom_image: str
:param master_machine_type: Compute engine machine type to use for the master node
:type master_machine_type: str
:param master_disk_type: Type of the boot disk for the master node
    (default is ``pd-standard``).
    Valid values: ``pd-ssd`` (Persistent Disk Solid State Drive) or
    ``pd-standard`` (Persistent Disk Hard Disk Drive).
:type master_disk_type: str
:param master_disk_size: Disk size for the master node
:type master_disk_size: int
:param worker_machine_type: Compute engine machine type to use for the worker nodes
:type worker_machine_type: str
:param worker_disk_type: Type of the boot disk for the worker node
    (default is ``pd-standard``).
    Valid values: ``pd-ssd`` (Persistent Disk Solid State Drive) or
    ``pd-standard`` (Persistent Disk Hard Disk Drive).
:type worker_disk_type: str
:param worker_disk_size: Disk size for the worker nodes
:type worker_disk_size: int
:param num_preemptible_workers: The # of preemptible worker nodes to spin up
:type num_preemptible_workers: int
:param labels: dict of labels to add to the cluster
:type labels: dict
:param network_uri: The network uri to be used for machine communication, cannot be
    specified with subnetwork_uri
:type network_uri: str
:param subnetwork_uri: The subnetwork uri to be used for machine communication,
    cannot be specified with network_uri
:type subnetwork_uri: str
:param internal_ip_only: If true, all instances in the cluster will only
    have internal IP addresses. This can only be enabled for subnetwork
    enabled networks
:type internal_ip_only: bool
:param tags: The GCE tags to add to all instances
:type tags: list[string]
:param google_cloud_conn_id: The connection ID to use connecting to Google Cloud Platform.
:type google_cloud_conn_id: str
:param delegate_to: The account to impersonate, if any.
    For this to work, the service account making the request must have domain-wide
    delegation enabled.
:type delegate_to: str
:param service_account: The service account of the dataproc instances.
:type service_account: str
:param service_account_scopes: The URIs of service account scopes to be included.
:type service_account_scopes: list[string]
:param idle_delete_ttl: The longest duration that cluster would keep alive while
    staying idle. Passing this threshold will cause cluster to be auto-deleted.
    A duration in seconds.
:type idle_delete_ttl: int
:param auto_delete_time:  The time when cluster will be auto-deleted.
:type auto_delete_time: datetime.datetime
:param auto_delete_ttl: The life duration of cluster, the cluster will be
    auto-deleted at the end of this duration.
    A duration in seconds. (If auto_delete_time is set this parameter will be ignored)
:type auto_delete_ttl: int
"""
def NessDataprocClusterCreate(
    dag,
    num_workers=DEFAULT_CLUSTER.get('num_workers'),
    network_uri=None,
    subnetwork_uri=None,
    internal_ip_only=None,
    tags=None,
    init_action_timeout="10m",
    custom_image=None,
    image_version=None,
    master_machine_type=DEFAULT_CLUSTER.get('master_machine_type'),
    master_disk_type=DEFAULT_CLUSTER.get('master_disk_type'),
    master_disk_size=DEFAULT_CLUSTER.get('master_disk_size'),
    worker_machine_type=DEFAULT_CLUSTER.get('worker_machine_type'),
    worker_disk_type=DEFAULT_CLUSTER.get('worker_disk_type'),
    worker_disk_size=DEFAULT_CLUSTER.get('worker_disk_size'),
    num_preemptible_workers=DEFAULT_CLUSTER.get('num_preemptible_workers'),
    labels=None,
    google_cloud_conn_id='google_cloud_default',
    delegate_to=None,
    service_account=None,
    service_account_scopes=None,
    idle_delete_ttl=None,
    auto_delete_time=None,
    auto_delete_ttl=None):

    return DataprocClusterCreateOperator(
        task_id='DataprocClusterCreate',
        cluster_name=_infer_cluster_name(dag.owner, dag.dag_id),
        project_id=DEFAULT_CLUSTER.get('project'),
        num_workers=num_workers,
        zone = DEFAULT_CLUSTER.get('zone'),
        storage_bucket=DEFAULT_CLUSTER.get('storage_bucket'),
        init_actions_uris=DEFAULT_CLUSTER.get('init_actions_uris'),
        metadata=DEFAULT_CLUSTER.get('metadata'),
        properties = DEFAULT_CLUSTER.get('properties'),
        master_machine_type=master_machine_type,
        master_disk_size=master_disk_size,
        worker_machine_type=worker_machine_type,
        worker_disk_size=worker_disk_size,
        num_preemptible_workers=num_preemptible_workers,
        labels=labels,
        region = DEFAULT_CLUSTER.get('region'),
        google_cloud_conn_id=google_cloud_conn_id,
        delegate_to=delegate_to)


"""
Delete a cluster on Google Cloud Dataproc. The operator will wait until the
cluster is destroyed.
:param google_cloud_conn_id: The connection ID to use connecting to Google Cloud Platform.
:type google_cloud_conn_id: str
:param delegate_to: The account to impersonate, if any.
    For this to work, the service account making the request must have domain-wide
    delegation enabled.
:type delegate_to: str
"""
def NessDataprocClusterDelete(
    dag,
    google_cloud_conn_id='google_cloud_default',
    delegate_to=None):
        return DataprocClusterDeleteOperator(
            task_id='DataprocClusterDelete',
            cluster_name=_infer_cluster_name(dag.owner, dag.dag_id),
            project_id = DEFAULT_CLUSTER.get('project'),
            region = DEFAULT_CLUSTER.get('region'),
            google_cloud_conn_id=google_cloud_conn_id,
            delegate_to=delegate_to,
            trigger_rule=TriggerRule.ALL_DONE)


def _infer_cluster_name(owner, dag_id):
    return owner.replace(' ','-').lower() + '-' + dag_id.lower() + '-cluster'
