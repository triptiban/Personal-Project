from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

K8S_CONFIG = "/opt/airflow/.kube/config"
K8S_CONTEXT = "kind-ecommerce-pipeline"   # change if your context differs

# Images (set in Airflow > Admin > Variables if you want to override)
EXTRACTOR_IMAGE   = Variable.get("EXTRACTOR_IMAGE",   default_var="extractor:0.2")
LOADER_IMAGE      = Variable.get("LOADER_IMAGE",      default_var="loader:0.2")
DBT_RUNNER_IMAGE  = Variable.get("DBT_RUNNER_IMAGE",  default_var="dbt-runner:0.1")
EXPORTER_IMAGE    = Variable.get("EXPORTER_IMAGE",    default_var="exporter:0.1")

# K8s settings
NAMESPACE = "ecommerce"
SERVICE_ACCOUNT = "default"

default_args = {"owner": "airflow", "start_date": datetime(2025, 1, 1)}

with DAG(
    dag_id="ecommerce_pipeline",
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["ecommerce"],
) as dag:

    # 1) Extractor -> writes raw JSON to MinIO (raw/date=YYYY-MM-DD/...)
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
            {"secret_ref":     {"name": "minio-secrets"}},
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        service_account_name=SERVICE_ACCOUNT,
        config_file=K8S_CONFIG,
        cluster_context=K8S_CONTEXT,
        image_pull_policy="IfNotPresent",
    )

    # 2) Loader -> reads MinIO raw and loads Postgres staging/analytics
    load_warehouse = KubernetesPodOperator(
        task_id="load_warehouse",
        name="loader",
        namespace=NAMESPACE,
        image=LOADER_IMAGE,
        cmds=["python"],
        arguments=["/app/load.py"],
        env_from=[
            {"config_map_ref": {"name": "minio-config"}},
            {"secret_ref":     {"name": "minio-secrets"}},
            {"secret_ref":     {"name": "postgres-secrets"}},  # << use your actual PG secret
        ],
        get_logs=True,
        is_delete_operator_pod=True,
        service_account_name=SERVICE_ACCOUNT,
        config_file=K8S_CONFIG,
        cluster_context=K8S_CONTEXT,
        image_pull_policy="IfNotPresent",
    )

    # 3) dbt -> builds analytics/marts in Postgres (incremental fact)
    run_dbt = KubernetesPodOperator(
        task_id="run_dbt",
        name="dbt-runner",
        namespace=NAMESPACE,
        image=DBT_RUNNER_IMAGE,
        cmds=["bash", "-lc"],
        arguments=[
            "dbt deps && dbt run --project-dir /work/ecommerce_dbt --profiles-dir /work/ecommerce_dbt/.dbt"
        ],
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

    # 4) Exporter -> exports Postgres analytics/marts to MinIO curated
    export_curated = KubernetesPodOperator(
        task_id="export_curated",
        name="exporter",
        namespace=NAMESPACE,
        image=EXPORTER_IMAGE,
        # If your image already has an ENTRYPOINT/CMD, omit cmds/arguments.
        # Uncomment if you need to call a script explicitly:
        # cmds=["python"],
        # arguments=["/app/export.py"],
        env_vars={
            "MINIO_ENDPOINT": "minio.ecommerce.svc.cluster.local:9000",
            "MINIO_BUCKET": "ecommerce",
            "CURATED_PREFIX": "curated",
        },
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

    # Correct order
    extract_raw >> load_warehouse >> run_dbt >> export_curated
