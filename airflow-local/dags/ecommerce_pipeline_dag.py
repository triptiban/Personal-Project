from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# Where kubeconfig is mounted by docker-compose:
K8S_CONFIG  = "/opt/airflow/.kube/config"
K8S_CONTEXT = "kind-ecommerce-pipeline"  # update if your context differs
NAMESPACE   = "ecommerce"
SERVICE_ACCOUNT = "default"

# Airflow Variables (set in UI: Admin -> Variables)
# Use the same tags you used successfully with kubectl Jobs
EXTRACTOR_IMAGE   = Variable.get("EXTRACTOR_IMAGE", default_var="extractor:0.2")
LOADER_IMAGE      = Variable.get("LOADER_IMAGE",    default_var="loader:0.1")
DBT_RUNNER_IMAGE  = Variable.get("DBT_RUNNER_IMAGE",default_var="dbt-runner:0.1")
EXPORTER_IMAGE    = Variable.get("EXPORTER_IMAGE",  default_var="exporter:0.1")

default_args = {"start_date": datetime(2025, 1, 1)}
with DAG(
    dag_id="ecommerce_pipeline_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["ecommerce"],
) as dag:

    # 1) Extractor -> writes raw to MinIO
    extract_raw = KubernetesPodOperator(
        task_id="extract_raw",
        name="extractor",
        namespace=NAMESPACE,
        image=EXTRACTOR_IMAGE,
        cmds=["python"],
        arguments=["/app/app.py"],
        env_from=[
            {"config_map_ref": {"name": "minio-config"}},
            {"secret_ref":     {"name": "minio-secrets"}},
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        service_account_name=SERVICE_ACCOUNT,
        config_file=K8S_CONFIG,
        cluster_context=K8S_CONTEXT,
        image_pull_policy="IfNotPresent",
    )

    # 2) Loader -> loads Postgres from MinIO raw
    load_warehouse = KubernetesPodOperator(
        task_id="load_warehouse",
        name="loader",
        namespace=NAMESPACE,
        image=LOADER_IMAGE,
        env_from=[
            {"config_map_ref": {"name": "minio-config"}},
            {"secret_ref":     {"name": "minio-secrets"}},
            {"secret_ref":     {"name": "postgres-secrets"}},
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        service_account_name=SERVICE_ACCOUNT,
        config_file=K8S_CONFIG,
        cluster_context=K8S_CONTEXT,
        image_pull_policy="IfNotPresent",
    )

    # 3) dbt -> builds analytics/marts in Postgres
    run_dbt = KubernetesPodOperator(
        task_id="run_dbt",
        name="dbt-runner",
        namespace=NAMESPACE,
        image=DBT_RUNNER_IMAGE,
        cmds=["bash","-lc"],
        arguments=["dbt deps && dbt run --project-dir /work/ecommerce_dbt --profiles-dir /work/ecommerce_dbt/.dbt"],
        env_from=[
            {"secret_ref": {"name": "postgres-secrets"}},
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        service_account_name=SERVICE_ACCOUNT,
        config_file=K8S_CONFIG,
        cluster_context=K8S_CONTEXT,
        image_pull_policy="IfNotPresent",
    )

    # 4) Exporter -> writes curated to MinIO from Postgres analytics/marts
    export_curated = KubernetesPodOperator(
        task_id="export_curated",
        name="exporter",
        namespace=NAMESPACE,
        image=EXPORTER_IMAGE,
        env=[
            {"name":"MINIO_ENDPOINT", "value":"minio.ecommerce.svc.cluster.local:9000"},
            {"name":"MINIO_BUCKET",   "value":"ecommerce"},
            {"name":"CURATED_PREFIX", "value":"curated"},
        ],
        env_from=[
            {"secret_ref": {"name": "minio-secrets"}},
            {"secret_ref": {"name": "postgres-secrets"}},
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        service_account_name=SERVICE_ACCOUNT,
        config_file=K8S_CONFIG,
        cluster_context=K8S_CONTEXT,
        image_pull_policy="IfNotPresent",
    )

    extract_raw >> load_warehouse >> run_dbt >> export_curated
