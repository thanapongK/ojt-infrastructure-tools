import os

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


RUN_SCRIPT_TEMPLATE = """
set -e
VENV_DIR="/app/.venv"

if [ ! -d "$VENV_DIR" ] || [ -z "$(ls -A $VENV_DIR)" ]; then
  echo "Virtual environment is missing or empty. Creating a new one..."
  python -m venv $VENV_DIR
else
  echo "Existing virtual environment found. Ensuring it's up to date..."
  python -m venv --upgrade $VENV_DIR
fi

echo "Activating virtual environment..."
source $VENV_DIR/bin/activate

echo "Installing dependencies... (if any)"
{pip_install_commands}

echo "Running the application..."
PYTHONPATH=/app/src python /app/src/main.py --dag-id {dag_id}{arguments}
"""

ENVIRONMENT_KEYS = [
    "AZURE_AUTHORITY_HOST",
    "AZURE_CLIENT_ID",
    "AZURE_FEDERATED_TOKEN_FILE",
    "AZURE_KEYVAULT_ENABLED",
    "AZURE_KEYVAULT_ENDPOINT",
    "AZURE_KEYVAULT_PRIVATE_ENDPOINT",
    "AZURE_TENANT_ID",
    "AZURE_WORKLOAD_IDENTITY",
    "ENVIRONMENT",
    "SPARK_EXECUTOR_IMAGE",
    "SPARK_EXECUTOR_NAMESPACE",
    "SPARK_EXECUTOR_NODE_SELECTOR_LABEL",
    "SPARK_EXECUTOR_NODE_SELECTOR_VALUE",
]


def build_run_script(dag_id: str, **kwargs) -> str:
    wrapped_dependencies = []

    for package in kwargs.get("dependencies", []):
        wrapped_dependencies.append(f'"{package}"')

    pip_install_commands = (
        f"pip install {' '.join(wrapped_dependencies)}"
        if len(wrapped_dependencies) > 0
        else "echo 'No dependencies to install.'"
    )

    parameter = kwargs.get("parameters", {})
    parameter_str = ""

    for key, value in parameter.items():
        parameter_str += f" --{key} {value}"

    return RUN_SCRIPT_TEMPLATE.format(
        pip_install_commands=pip_install_commands,
        dag_id=dag_id,
        arguments=parameter_str,
    )


def merge_volumes(default_values: list[k8s.V1Volume], values: list[k8s.V1Volume]):
    value_dict = {value.name: value for value in default_values}

    for value in values:
        value_dict[value.name] = value

    return list(value_dict.values())


def merge_volume_mounts(
    default_values: list[k8s.V1VolumeMount], values: list[k8s.V1VolumeMount]
):
    value_dict = {value.mount_path: value for value in default_values}

    for value in values:
        value_dict[value.mount_path] = value

    return list(value_dict.values())


def build_kubernetes_pod_operator(
    dag_id: str,
    task_id: str,
    name: str,
    volumes: list[k8s.V1Volume],
    volume_mounts: list[k8s.V1VolumeMount],
    resources: k8s.V1ResourceRequirements | None = None,
    config: dict = {},
    arguments: dict = {},
) -> KubernetesPodOperator:
    """
    Builds a KubernetesPodOperator for Spark driver pod.

    **Remarks**:
        To pass additional parameters to the execution script, add it as key-value to arguments in format (e.g. { "--some-arguments": "someValue" })
        Except these values: dependencies, dag id which are auto-handled via --dag-id parameter

    Args:
        dag_id (str): The DAG ID.
        task_id (str): The task ID.
        name (str): The name of the pod.
        volumes (list[k8s.V1Volume]): List of Kubernetes volumes to mount.
        volume_mounts (list[k8s.V1VolumeMount]): List of Kubernetes volume mounts.
        resources (k8s.V1ResourceRequirements | None, optional): Resource requirements for the pod. Defaults to None.
        config (dict, optional): Configuration dictionary for the pod. Defaults to {}.
        arguments (dict, optional): Additional arguments for the run script. Defaults to {}.

    Returns:
        KubernetesPodOperator: Configured KubernetesPodOperator instance.
    """

    driver_image = config.get("image", "spark:latest")
    driver_namespace = config.get("namespace")
    driver_service_account_name = config.get("serviceAccountName")
    driver_annotations = config.get("annotations")
    driver_node_selector = config.get("nodeSelector")
    driver_on_finish_action = config.get("onFinishAction", "keep_pod")

    driver_env_vars = [
        k8s.V1EnvVar(name=var_name, value=os.getenv(var_name))
        for var_name in ENVIRONMENT_KEYS
    ]
    driver_env_vars.extend(
        [
            k8s.V1EnvVar(
                name="SPARK_EXECUTOR_IMAGE",
                value=os.getenv("SPARK_EXECUTOR_IMAGE", driver_image),
            ),
            k8s.V1EnvVar(
                name="SPARK_EXECUTOR_SERVICE_ACCOUNT",
                value="default",
            ),
            # k8s.V1EnvVar(
            #     name="HOME",
            #     value="/home/spark",
            # ),
        ]
    )

    shared_pvc_name = config.get("pvc")

    default_volumes = [
        k8s.V1Volume(
            name="shared",
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name=shared_pvc_name,
            ),
        ),
        k8s.V1Volume(name="tmp", empty_dir=k8s.V1EmptyDirVolumeSource()),
        k8s.V1Volume(name="local", empty_dir=k8s.V1EmptyDirVolumeSource()),
        k8s.V1Volume(
            name="kube-api-access",
            projected=k8s.V1ProjectedVolumeSource(
                sources=[
                    k8s.V1VolumeProjection(
                        service_account_token=k8s.V1ServiceAccountTokenProjection(
                            expiration_seconds=3607,
                            path="token",
                        )
                    ),
                    k8s.V1VolumeProjection(
                        config_map=k8s.V1ConfigMapProjection(
                            name="kube-root-ca.crt",
                            items=[k8s.V1KeyToPath(key="ca.crt", path="ca.crt")],
                        )
                    ),
                    k8s.V1VolumeProjection(
                        downward_api=k8s.V1DownwardAPIProjection(
                            items=[
                                k8s.V1DownwardAPIVolumeFile(
                                    path="namespace",
                                    field_ref=k8s.V1ObjectFieldSelector(
                                        api_version="v1",
                                        field_path="metadata.namespace",
                                    ),
                                )
                            ]
                        )
                    ),
                ],
                default_mode=420,
            ),
        ),
    ]

    default_volume_mounts = [
        k8s.V1VolumeMount(
            name="shared",
            mount_path="/home/spark/.ivy2",
            sub_path="app-ivy2-dir",
        ),
        k8s.V1VolumeMount(name="tmp", mount_path="/tmp"),
        k8s.V1VolumeMount(
            name="local",
            mount_path="/home/spark/.local",
        ),
        k8s.V1VolumeMount(
            name="kube-api-access",
            mount_path="/var/run/secrets/kubernetes.io/serviceaccount",
            read_only=True,
        ),
    ]

    actual_volumes = merge_volumes(default_volumes, volumes)
    actual_volume_mounts = merge_volume_mounts(default_volume_mounts, volume_mounts)

    # set container resources
    default_container_resources = k8s.V1ResourceRequirements(
        requests={"cpu": "1", "memory": "8Gi", "ephemeral-storage": "8Gi"},
        limits={"cpu": "1", "memory": "8Gi", "ephemeral-storage": "8Gi"},
    )

    actual_container_resources = (
        default_container_resources if resources is None else resources
    )

    return KubernetesPodOperator(
        task_id=task_id,
        name=name,
        startup_timeout_seconds=600,
        get_logs=True,
        in_cluster=True,
        on_finish_action=driver_on_finish_action,
        namespace=driver_namespace,
        image=driver_image,
        base_container_name="spark-driver",
        labels={"azure.workload.identity/use": "true"},
        annotations=driver_annotations,
        service_account_name=driver_service_account_name,
        node_selector=driver_node_selector,
        env_vars=driver_env_vars,
        security_context=k8s.V1PodSecurityContext(
            run_as_user=185,
            run_as_non_root=True,
        ),
        container_security_context=k8s.V1SecurityContext(
            run_as_user=185,
            run_as_group=185,
            run_as_non_root=True,
            allow_privilege_escalation=False,
            read_only_root_filesystem=True,
        ),
        # automount_service_account_token=False, # TODO: need to further check this with Airflow 2.10+ and 3.0+
        container_resources=actual_container_resources,
        volumes=actual_volumes,
        volume_mounts=actual_volume_mounts,
        cmds=["/bin/bash"],
        arguments=[
            "-c",
            build_run_script(
                dag_id=dag_id,
                dependencies=arguments.pop("dependencies", []),
                **arguments,
            ),
        ],
    )


def parse_kubernetes_config(config: dict):
    service_account_name = config.get(
        "serviceAccountName", os.getenv("SPARK_DRIVER_SERVICE_ACCOUNT_NAME", "")
    )

    spark_driver_namespace = config.get(
        "namespace", os.getenv("SPARK_DRIVER_NAMESPACE", "")
    )
    spark_image = config.get("image", os.getenv("SPARK_EXECUTOR_IMAGE", ""))
    spark_annotations = config.get("annotations", {"sidecar.istio.io/inject": "false"})
    spark_node_selector = config.get("nodeSelector", {})
    spark_source_pvc = config.get("pvc", "")
    spark_labels = config.get("labels", {})

    config = {
        "namespace": spark_driver_namespace,
        "serviceAccountName": service_account_name,
        "image": spark_image,
        "annotations": spark_annotations,
        "labels": spark_labels,
        "nodeSelector": spark_node_selector,
        "pvc": spark_source_pvc,
    }

    return config
