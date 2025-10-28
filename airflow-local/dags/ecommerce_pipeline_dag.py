from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

K8S_CONFIG = "/opt/airflow/.kube/config"
K8S_CONTEXT = "kind-ecommerce-pipeline"   # change if your context name differs

# Airflow Variables (set these below in step 3)
EXTRACTOR_IMAGE = Variable.get("EXTRACTOR_IMAGE", default_var="extractor:0.2")
DBT_RUNNER_IMAGE = Variable.get("DBT_RUNNER_IMAGE", default_var="dbt-runner:0.1")
LOADER_IMAGE = Variable.get("LOADER_IMAGE", default_var="loader:0.2")

# K8s settings
NAMESPACE = "ecommerce"
SERVICE_ACCOUNT = "default"  # or your SA if you created one
# If you created ConfigMap/Secret for MinIO/DB creds, reference them via env_from below.

default_args = {"owner": "airflow"}

with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["ecommerce"],
) as dag:

    extract_raw = KubernetesPodOperator(
        task_id="extract_raw",
        name="extractor",
        namespace=NAMESPACE,
        image=EXTRACTOR_IMAGE,
        cmds=["python"],
        arguments=["/app/app.py"],
        env_vars={"API_BASE": "https://fakestoreapi.com"},
        env_from=[
            {"config_map_ref": {"name": "minio-config"}},
            {"secret_ref": {"name": "minio-secrets"}},
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        service_account_name=SERVICE_ACCOUNT,
        config_file=K8S_CONFIG,
        cluster_context=K8S_CONTEXT,
    )

    run_dbt = KubernetesPodOperator(
        task_id="run_dbt",
        name="dbt-runner",
        namespace=NAMESPACE,
        image=DBT_RUNNER_IMAGE,
        cmds=["bash", "-lc"],
        arguments=["dbt deps && dbt run --project-dir /work/ecommerce_dbt --profiles-dir /work/ecommerce_dbt/.dbt"],
        env_from=[
            {"config_map_ref": {"name": "warehouse-config"}},
            {"secret_ref": {"name": "warehouse-secrets"}},
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        service_account_name=SERVICE_ACCOUNT,
        config_file=K8S_CONFIG,
        cluster_context=K8S_CONTEXT,
    )

    load_warehouse = KubernetesPodOperator(
        task_id="load_warehouse",
        name="loader",
        namespace=NAMESPACE,
        image=LOADER_IMAGE,
        cmds=["python"],
        arguments=["/app/load.py"],
        env_from=[
            {"config_map_ref": {"name": "minio-config"}},
            {"secret_ref": {"name": "minio-secrets"}},
            {"config_map_ref": {"name": "warehouse-config"}},
            {"secret_ref": {"name": "warehouse-secrets"}},
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        service_account_name=SERVICE_ACCOUNT,
        config_file=K8S_CONFIG,
        cluster_context=K8S_CONTEXT,
    )

    extract_raw >> run_dbt >> load_warehouse
