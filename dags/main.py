from datetime import timedelta
import logging
import os
from pathlib import Path
import sys
from time import time

from airflow.decorators import dag, task_group
from kubernetes.client import models as k8s
from pyeqx.common import helper

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/bitnami/airflow")
BASE_DIR = f"{AIRFLOW_HOME}/dags"

full_path = helper.get_directory_name(__file__)
parent_path = full_path.split(BASE_DIR)[1]

main_dir = str(Path(parent_path).parents[0])
project_name = main_dir.lstrip("/")

PROJECT_NAME = project_name
DAG_ID = PROJECT_NAME

HOME_DIR = f"{BASE_DIR}{main_dir}"
WORKING_DIR = f"{HOME_DIR}"

VERSION = helper.open_file_as_text(f"{HOME_DIR}/VERSION")
PROJECT_TAG = "infrastructure-testing-tools"
PROJECT_DESC = f"Infrastructure Testing Tools {VERSION}"
PROJECT_OWNER = "cronus-esbm-cde"
KERNEL_NAME = f"python-3-13-{PROJECT_TAG}"

sys.path.append(WORKING_DIR)
sys.path.append(f"{WORKING_DIR}/dags")

config_path = f"{WORKING_DIR}/config/config.json"
config = helper.open_file_as_json(path=config_path)
config_dag: dict = config.get(
    "dag",
    {
        "retries": 0,
        "retryDelay": timedelta(minutes=5),
        "scheduleInterval": None,
        "maxActiveRuns": 1,
        "tags": [],
    },
)

logger = logging.getLogger(__name__)


@dag(
    dag_id=DAG_ID,
    description=PROJECT_DESC,
    tags=[DAG_ID, PROJECT_TAG, VERSION],
    default_args={
        "owner": PROJECT_OWNER,
    },
    start_date=None,
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
)
def execute():
    from dags.utils import build_kubernetes_pod_operator, parse_kubernetes_config

    config_k8s_raw: dict = config.get("kubernetes", {})
    config_k8s = parse_kubernetes_config(config=config_k8s_raw)

    python_dependencies: list[str] = config.get("dependencies", [])

    pod_name_prefix = f"{PROJECT_NAME}-{int(time())}"

    shared_volume_mounts = [
        k8s.V1VolumeMount(
            name="shared",
            mount_path="/app/src",
            sub_path=f"app-dags-dir/{PROJECT_NAME}/src",
            read_only=True,
        ),
        k8s.V1VolumeMount(
            name="shared",
            mount_path="/app/notebooks",
            sub_path=f"app-dags-dir/{PROJECT_NAME}/notebooks",
            read_only=True,
        ),
        k8s.V1VolumeMount(
            name="shared",
            mount_path="/app/VERSION",
            sub_path=f"app-dags-dir/{PROJECT_NAME}/VERSION",
            read_only=True,
        ),
        k8s.V1VolumeMount(
            name="shared",
            mount_path="/app/config",
            sub_path=f"app-dags-dir/{PROJECT_NAME}/config",
            read_only=True,
        ),
        k8s.V1VolumeMount(
            name="shared",
            mount_path="/app/helper",
            sub_path=f"app-dags-dir/{PROJECT_NAME}/helper",
            read_only=True,
        ),
    ]

    @task_group(group_id="infrastructure-tools-ojt-operations")
    def infrastructure_tools_ojt_operations():
        volume_mounts = shared_volume_mounts + [
            k8s.V1VolumeMount(
                name="shared",
                mount_path="/app/.venv",
                sub_path=f"app-venv-dir/{KERNEL_NAME}-spark-3-5",  # TODO: Review how to change this
            ),
        ]

        task_minio_ops = build_kubernetes_pod_operator(
            dag_id=DAG_ID,
            task_id="minio_operations",
            name=f"{pod_name_prefix}-minio-ops",
            volumes=[],
            volume_mounts=volume_mounts,
            config=config_k8s,
            arguments={
                "dependencies": python_dependencies,
                "parameters": {"task": "minio_operations"},
            },
        )

        task_postgres_ops = build_kubernetes_pod_operator(
            dag_id=DAG_ID,
            task_id="postgres_operations",
            name=f"{pod_name_prefix}-postgres-ops",
            volumes=[],
            volume_mounts=volume_mounts,
            config=config_k8s,
            arguments={
                "dependencies": python_dependencies,
                "parameters": {"task": "postgres_operations"},
            },
        )

        task_mongodb_ops = build_kubernetes_pod_operator(
            dag_id=DAG_ID,
            task_id="mongodb_operations",
            name=f"{pod_name_prefix}-mongodb-ops",
            volumes=[],
            volume_mounts=volume_mounts,
            config=config_k8s,
            arguments={
                "dependencies": python_dependencies,
                "parameters": {"task": "mongodb_operations"},
            },
        )

        task_cross_db_ops = build_kubernetes_pod_operator(
            dag_id=DAG_ID,
            task_id="cross_database_operations",
            name=f"{pod_name_prefix}-cross-db-ops",
            volumes=[],
            volume_mounts=volume_mounts,
            config=config_k8s,
            arguments={
                "dependencies": python_dependencies,
                "parameters": {"task": "cross_database_operations"},
            },
        )

        return [
            task_minio_ops,
            task_postgres_ops,
            task_mongodb_ops,
        ] >> task_cross_db_ops

    infrastructure_tools_ojt_operations()


execute()
