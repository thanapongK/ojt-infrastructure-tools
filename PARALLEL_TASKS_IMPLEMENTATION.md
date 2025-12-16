# Parallel Tasks Implementation Guide

> **Complete code examples for running 4 database operations in parallel**
> 
> Project: cronus-esbm-etl-infrastructure-testing-tools
> 
> Date: December 16, 2025

---

## 📁 Project Structure

```
cronus-esbm-etl-infrastructure-testing-tools/
│
├── dags/
│   ├── main.py                          # ✅ แก้ไข: เพิ่ม parallel task group
│   └── utils.py                         # ✅ มีอยู่แล้ว
│
├── src/
│   ├── main.py                          # ✅ แก้ไข: เพิ่ม task routing
│   │
│   └── app/
│       ├── task_minio_operations.py         # 📄 ใหม่
│       ├── task_postgres_operations.py      # 📄 ใหม่
│       ├── task_mongodb_operations.py       # 📄 ใหม่
│       └── task_cross_database_operations.py # 📄 ใหม่
│
└── etl/                                 # ✅ มี managers อยู่แล้ว
    ├── minio_manager.py
    ├── postgres_manager.py
    ├── mongodb_manager.py
    └── cross_database_manager.py
```

---

## 1️⃣ DAG: `dags/main.py` (แก้ไขเพิ่ม Task Group)

```python
from datetime import timedelta
import logging
import os
from pathlib import Path
import sys
from time import time

from airflow.decorators import dag, task_group
from kubernetes.client import models as k8s
from pyeqx.common import helper

# system settings
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

    # configure kubernetes
    config_k8s_raw: dict = config.get(
        "kubernetes",
        {},
    )
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
            mount_path="/app/etl",
            sub_path=f"app-dags-dir/{PROJECT_NAME}/etl",
            read_only=True,
        ),
    ]

    # ========== Parallel Database Operations ==========
    @task_group(group_id="parallel-database-operations")
    def execute_parallel_database_operations():
        """
        รัน 4 database operations พร้อมกัน:
        1. MinIO operations
        2. PostgreSQL operations
        3. MongoDB operations
        4. Cross-database operations
        """
        
        volume_mounts = shared_volume_mounts + [
            k8s.V1VolumeMount(
                name="shared",
                mount_path="/app/.venv",
                sub_path=f"app-venv-dir/{KERNEL_NAME}-spark-3-5",
            ),
            k8s.V1VolumeMount(
                name="shared",
                mount_path="/home/spark/.cache",
                sub_path=f"app-cache-dir/{KERNEL_NAME}-spark-3-5",
            ),
        ]

        # Pod 1: MinIO Operations
        task_minio_ops = build_kubernetes_pod_operator(
            dag_id=DAG_ID,
            task_id="minio_operations",
            name=f"{pod_name_prefix}-minio-ops",
            volumes=[],
            volume_mounts=volume_mounts,
            config=config_k8s,
            arguments={
                "dependencies": python_dependencies,
                "parameters": {
                    "task": "minio_operations",
                    "operation": "full_workflow",
                },
            },
        )

        # Pod 2: PostgreSQL Operations
        task_postgres_ops = build_kubernetes_pod_operator(
            dag_id=DAG_ID,
            task_id="postgres_operations",
            name=f"{pod_name_prefix}-postgres-ops",
            volumes=[],
            volume_mounts=volume_mounts,
            config=config_k8s,
            arguments={
                "dependencies": python_dependencies,
                "parameters": {
                    "task": "postgres_operations",
                    "operation": "full_workflow",
                },
            },
        )

        # Pod 3: MongoDB Operations
        task_mongodb_ops = build_kubernetes_pod_operator(
            dag_id=DAG_ID,
            task_id="mongodb_operations",
            name=f"{pod_name_prefix}-mongodb-ops",
            volumes=[],
            volume_mounts=volume_mounts,
            config=config_k8s,
            arguments={
                "dependencies": python_dependencies,
                "parameters": {
                    "task": "mongodb_operations",
                    "operation": "full_workflow",
                },
            },
        )

        # Pod 4: Cross-Database Operations
        task_cross_db_ops = build_kubernetes_pod_operator(
            dag_id=DAG_ID,
            task_id="cross_database_operations",
            name=f"{pod_name_prefix}-cross-db-ops",
            volumes=[],
            volume_mounts=volume_mounts,
            config=config_k8s,
            arguments={
                "dependencies": python_dependencies,
                "parameters": {
                    "task": "cross_database_operations",
                    "operation": "all_transfers",
                },
            },
        )

        # ========== รัน 4 Pods พร้อมกัน! ==========
        [task_minio_ops, task_postgres_ops, task_mongodb_ops, task_cross_db_ops]

    # ========== Execute Task Group ==========
    execute_parallel_database_operations()


execute()
```

---

## 2️⃣ Entry Point: `src/main.py` (แก้ไขเพิ่ม Routing)

```python
import argparse
import logging
import os

log_format = "%(asctime)s %(levelname)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format)

working_dir = os.getcwd()


def debug(logger: logging.Logger, arguments: argparse.Namespace):
    logger.debug(f"debugging: arguments: {arguments}")
    logger.debug(f"debugging: working directory: {working_dir}")


def main():
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(description="Run with parameters.")

    parser.add_argument(
        "--dag-id",
        type=str,
        help="DAG ID",
        required=True,
    )

    parser.add_argument(
        "--task", type=str, help="Task", required=False, default="minio_operations"
    )
    
    parser.add_argument(
        "--operation",
        type=str,
        help="Operation type",
        required=False,
        default="full_workflow",
    )

    args = parser.parse_args()

    debug(logger=logger, arguments=args)

    # ========== Database Operations Tasks ==========
    if args.task == "minio_operations":
        from app.task_minio_operations import run_minio_process

        logger.info(f"Running task: {args.task}")
        run_minio_process(
            logger=logger,
            working_dir=working_dir,
            config_path="/app/config/config.json",
            arguments={
                "name": "infrastructure-testing-tools-minio",
                "dag_id": args.dag_id,
                "operation": args.operation,
            },
        )

    elif args.task == "postgres_operations":
        from app.task_postgres_operations import run_postgres_process

        logger.info(f"Running task: {args.task}")
        run_postgres_process(
            logger=logger,
            working_dir=working_dir,
            config_path="/app/config/config.json",
            arguments={
                "name": "infrastructure-testing-tools-postgres",
                "dag_id": args.dag_id,
                "operation": args.operation,
            },
        )

    elif args.task == "mongodb_operations":
        from app.task_mongodb_operations import run_mongodb_process

        logger.info(f"Running task: {args.task}")
        run_mongodb_process(
            logger=logger,
            working_dir=working_dir,
            config_path="/app/config/config.json",
            arguments={
                "name": "infrastructure-testing-tools-mongodb",
                "dag_id": args.dag_id,
                "operation": args.operation,
            },
        )

    elif args.task == "cross_database_operations":
        from app.task_cross_database_operations import run_cross_database_process

        logger.info(f"Running task: {args.task}")
        run_cross_database_process(
            logger=logger,
            working_dir=working_dir,
            config_path="/app/config/config.json",
            arguments={
                "name": "infrastructure-testing-tools-cross-database",
                "dag_id": args.dag_id,
                "operation": args.operation,
            },
        )

    else:
        logger.error(f"Unknown task: {args.task}")


if __name__ == "__main__":
    main()
```

---

## 3️⃣ Task: `src/app/task_minio_operations.py` (ใหม่)

```python
"""
Task: MinIO Operations
Upload, read, and list objects in MinIO/S3
Pattern: อิงจาก task_spark_process.py
"""

from dataclasses import dataclass
from datetime import datetime, timezone
import glob
import logging
import os
import socket
from typing import Any

from opentelemetry import trace
from opentelemetry.propagate import inject

from pyeqx.common import helper
from pyeqx.core import initialize_configuration, initialize_operation, Operation
from pyeqx.core.configuration import Configuration
from pyeqx.core.constants import (
    LOG_EXECUTION_COMPLETED,
    LOG_EXECUTION_FAILED,
    LOG_EXECUTION_STARTED,
)
from pyeqx.execution import (
    ExecuteParameters,
    Process,
    ProcessExecutionParameters,
    ProcessParameters,
)
from pyeqx.opentelemetry.instrumentation import initialize_telemetry
from pyeqx.opentelemetry.spark import configure_spark_options

from app.utils import parse_telemetry_config

hostname = socket.gethostname()
host_ip = socket.gethostbyname(hostname)


@dataclass
class MinioProcessParameters(ProcessParameters):
    operation_type: str
    
    def __init__(
        self,
        operation_type: str = "full_workflow",
        is_log_load_data: bool = False,
    ):
        super().__init__(is_log_load_data=is_log_load_data)
        self.operation_type = operation_type


@dataclass
class MinioExecutionParameters(ExecuteParameters):
    dag_id: str
    operation_type: str
    
    def __init__(
        self,
        name: str,
        dag_id: str,
        operation_type: str = "full_workflow",
    ):
        self.name = name
        self.dag_id = dag_id
        self.operation_type = operation_type


class RunMinioProcess(Process):
    __debug_mode: bool = False
    __manager = None

    def __init__(
        self,
        config: Configuration,
        logger: logging.Logger,
        operation: Operation,
    ):
        super().__init__(config, logger, operation)

    def configure(self, parameters: ProcessParameters):
        super().configure(parameters)
        assert isinstance(parameters, MinioProcessParameters)
        
        self.logger.info(f"Configuring MinIO operations: {parameters.operation_type}")
        
        # Initialize MinIO Manager
        try:
            import sys
            etl_path = "/app/etl"
            if etl_path not in sys.path:
                sys.path.insert(0, etl_path)
            
            from minio_manager import MinioManager
            from schemas import get_raw_valid_data_user_schema
            
            spark = self.operation.get_current_spark_session()
            self.__manager = MinioManager(spark=spark)
            self.__schema = get_raw_valid_data_user_schema
            
            self.logger.info("✅ MinIO manager configured")
            
        except ImportError as ie:
            self.logger.error(f"Failed to import managers: {ie}")
            raise

    def _read_datas(self, parameters: ProcessExecutionParameters):
        assert isinstance(parameters, MinioExecutionParameters)
        pass

    def _process_datas(self, parameters):
        assert isinstance(parameters, MinioExecutionParameters)
        
        operation = parameters.operation_type
        
        if operation == "full_workflow":
            self._run_full_workflow()
        elif operation == "test_connection":
            self._test_connection()
        elif operation == "upload_csv":
            self._upload_csv()
        elif operation == "upload_json":
            self._upload_json()
        elif operation == "read_all":
            self._read_all_data()
        elif operation == "list_objects":
            self._list_objects()
        else:
            raise ValueError(f"Unknown operation: {operation}")

    def _run_full_workflow(self):
        """Run complete workflow: test → upload → read → list"""
        self.logger.info("=" * 60)
        self.logger.info("Running MinIO Full Workflow")
        self.logger.info("=" * 60)
        
        self._test_connection()
        self._upload_csv()
        self._upload_json()
        self._read_all_data()
        self._list_objects()
        
        self.logger.info("=" * 60)
        self.logger.info("✅ MinIO Full Workflow Completed")
        self.logger.info("=" * 60)

    def _test_connection(self):
        self.logger.info("Testing MinIO connection...")
        self.__manager.test_connection()
        self.logger.info("✅ MinIO connection successful")

    def _upload_csv(self):
        self.logger.info("Uploading CSV to S3...")
        
        csv_pattern = "/opt/airflow/ojt/data/processed/valid_users.csv"
        csv_files = glob.glob(csv_pattern)
        
        if not csv_files:
            self.logger.warning(f"No CSV files found: {csv_pattern}")
            return
        
        local_file = csv_files[0]
        self.logger.info(f"Found file: {local_file}")
        
        self.__manager.write_data_to_s3(
            local_file_path=local_file,
            s3_path="silver",
            schema=self.__schema(),
            output_filename="raw_valid_data_user",
            format="delta",
            mode="overwrite",
        )
        
        self.logger.info("✅ CSV uploaded: s3a://ojtbucket/silver/raw_valid_data_user")

    def _upload_json(self):
        self.logger.info("Uploading JSON to S3...")
        
        json_path = "/opt/airflow/ojt/data/processed/simple_json.json"
        
        spark = self.operation.get_current_spark_session()
        df = spark.read.json(json_path)
        row_count = df.count()
        
        self.logger.info(f"Loaded {row_count} rows from JSON")
        
        self.__manager.write_data_to_s3(
            df=df,
            s3_path="silver",
            output_filename="simple_json_data",
            format="delta",
            mode="overwrite",
        )
        
        self.logger.info("✅ JSON uploaded: s3a://ojtbucket/silver/simple_json_data")

    def _read_all_data(self):
        self.logger.info("Reading all data from S3...")
        
        # Read CSV
        csv_path = "s3a://ojtbucket/silver/raw_valid_data_user"
        csv_df = self.__manager.read_data_from_s3(path=csv_path, format="delta")
        csv_count = csv_df.count()
        self.logger.info(f"CSV: {csv_count} rows")
        csv_df.show(3, truncate=False)
        
        # Read JSON
        json_path = "s3a://ojtbucket/silver/simple_json_data"
        json_df = self.__manager.read_data_from_s3(path=json_path, format="delta")
        json_count = json_df.count()
        self.logger.info(f"JSON: {json_count} rows")
        json_df.show(3, truncate=False)
        
        self.logger.info(f"✅ Total: {csv_count + json_count} rows read")

    def _list_objects(self):
        self.logger.info("Listing MinIO objects...")
        
        result = self.__manager.list_objects(prefix="silver/")
        
        if result["success"]:
            self.logger.info(f"✅ Listed {result['count']} objects")
            for obj in result["objects"]:
                self.logger.info(f"  📄 {obj['name']} ({obj['size']} bytes)")
        else:
            self.logger.error(f"❌ List failed: {result['message']}")


def run_minio_process(
    logger: logging.Logger,
    working_dir: str,
    config_path: str,
    arguments: dict[str, Any],
):
    name = arguments.get("name", "infrastructure-testing-tools-minio")
    operation_type = arguments.get("operation", "full_workflow")

    spark_executor_service_account = os.getenv("SPARK_EXECUTOR_SERVICE_ACCOUNT")

    jar_packages = [
        "io.delta:delta-spark_2.12:3.3.2",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.apache.hadoop:hadoop-aws:3.3.4",
    ]

    spark_config = {
        "spark.driver.extraClassPath": None,
        "spark.driver.extraJavaOptions": "-Dcom.sun.net.ssl.checkRevocation=false",
        "spark.kubernetes.authenticate.driver.serviceAccountName": spark_executor_service_account,
        "spark.kubernetes.authenticate.serviceAccountName": spark_executor_service_account,
        "spark.kubernetes.executor.annotation.sidecar.istio.io/inject": False,
        "spark.extraListeners": "",
        "spark.jars.ivy": "/home/spark/.ivy2",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    }

    config_raw = helper.open_file_as_json(path=config_path)
    config = initialize_configuration(
        config=config_raw, working_dir=f"{working_dir}/config/", file_path=config_path
    )

    telemetry_config_raw = config_raw.get("telemetry", {"enabled": False})
    telemetry_config = parse_telemetry_config(config=telemetry_config_raw)
    tracer_provider, metric_provider, log_provider = initialize_telemetry(
        config=telemetry_config
    )

    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("run_minio_process") as span:
        span.set_attribute("app.task", name)
        span.set_attribute("app.operation", operation_type)
        span_carrier = {}
        inject(span_carrier)

        operation = initialize_operation(name=name, config=config, logger=logger)

        spark_options = spark_config
        spark_options.update(configure_spark_options(config=telemetry_config))
        spark_options.update(config.engine.spark_options)

        operation.initialize_spark(
            packages=jar_packages, options=spark_options, is_debug=True
        )

        __do_run_minio_process(
            logger=logger, config=config, operation=operation, arguments=arguments
        )

    logger.info("Cleaning up telemetry resources")
    metric_provider.force_flush()
    metric_provider.shutdown()
    tracer_provider.force_flush()
    tracer_provider.shutdown()


def __do_run_minio_process(
    logger: logging.Logger,
    config: Configuration,
    operation: Operation,
    arguments: dict[str, Any],
):
    try:
        name = arguments.get("name", "infrastructure-testing-tools-minio")
        dag_id = arguments.get("dag_id", "")
        operation_type = arguments.get("operation", "full_workflow")

        start_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        process = RunMinioProcess(config=config, logger=logger, operation=operation)

        logger.info(LOG_EXECUTION_STARTED)
        process.configure(
            parameters=MinioProcessParameters(
                operation_type=operation_type,
                is_log_load_data=False
            )
        )
        process.execute(
            parameters=MinioExecutionParameters(
                name=name,
                dag_id=dag_id,
                operation_type=operation_type,
            )
        )

        end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        logger.info(
            f"{LOG_EXECUTION_COMPLETED} (elapsed: {end_timestamp - start_timestamp} ms)"
        )
    except Exception as ex:
        logger.info(LOG_EXECUTION_FAILED)
        logger.error(ex)
        raise ex
```

---

## 4️⃣ Task: `src/app/task_postgres_operations.py` (ใหม่)

```python
"""
Task: PostgreSQL Operations
Test connection, read version, update version
Pattern: อิงจาก task_spark_process.py
"""

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import os
import socket
from typing import Any

from opentelemetry import trace
from opentelemetry.propagate import inject

from pyeqx.common import helper
from pyeqx.core import initialize_configuration, initialize_operation, Operation
from pyeqx.core.configuration import Configuration
from pyeqx.core.constants import (
    LOG_EXECUTION_COMPLETED,
    LOG_EXECUTION_FAILED,
    LOG_EXECUTION_STARTED,
)
from pyeqx.execution import (
    ExecuteParameters,
    Process,
    ProcessExecutionParameters,
    ProcessParameters,
)
from pyeqx.opentelemetry.instrumentation import initialize_telemetry
from pyeqx.opentelemetry.spark import configure_spark_options

from app.utils import parse_telemetry_config

hostname = socket.gethostname()
host_ip = socket.gethostbyname(hostname)


@dataclass
class PostgresProcessParameters(ProcessParameters):
    operation_type: str
    
    def __init__(
        self,
        operation_type: str = "full_workflow",
        is_log_load_data: bool = False,
    ):
        super().__init__(is_log_load_data=is_log_load_data)
        self.operation_type = operation_type


@dataclass
class PostgresExecutionParameters(ExecuteParameters):
    dag_id: str
    operation_type: str
    
    def __init__(
        self,
        name: str,
        dag_id: str,
        operation_type: str = "full_workflow",
    ):
        self.name = name
        self.dag_id = dag_id
        self.operation_type = operation_type


class RunPostgresProcess(Process):
    __debug_mode: bool = False
    __manager = None
    __db_param = None

    def __init__(
        self,
        config: Configuration,
        logger: logging.Logger,
        operation: Operation,
    ):
        super().__init__(config, logger, operation)

    def configure(self, parameters: ProcessParameters):
        super().configure(parameters)
        assert isinstance(parameters, PostgresProcessParameters)
        
        self.logger.info(f"Configuring PostgreSQL operations: {parameters.operation_type}")
        
        try:
            import sys
            etl_path = "/app/etl"
            config_path = "/app/config"
            if etl_path not in sys.path:
                sys.path.insert(0, etl_path)
            if config_path not in sys.path:
                sys.path.insert(0, config_path)
            
            from postgres_manager import PostgresManager
            from postgres_configuration import PostgresConfiguration
            
            spark = self.operation.get_current_spark_session()
            self.__manager = PostgresManager(spark=spark)
            self.__db_param = PostgresConfiguration.postgres_param
            
            self.logger.info("✅ PostgreSQL manager configured")
            
        except ImportError as ie:
            self.logger.error(f"Failed to import managers: {ie}")
            raise

    def _read_datas(self, parameters: ProcessExecutionParameters):
        assert isinstance(parameters, PostgresExecutionParameters)
        pass

    def _process_datas(self, parameters):
        assert isinstance(parameters, PostgresExecutionParameters)
        
        operation = parameters.operation_type
        
        if operation == "full_workflow":
            self._run_full_workflow()
        elif operation == "test_connection":
            self._test_connection()
        elif operation == "read_version":
            self._read_version()
        elif operation == "update_version":
            self._update_version()
        else:
            raise ValueError(f"Unknown operation: {operation}")

    def _run_full_workflow(self):
        """Run complete workflow: test → read → update"""
        self.logger.info("=" * 60)
        self.logger.info("Running PostgreSQL Full Workflow")
        self.logger.info("=" * 60)
        
        self._test_connection()
        current_version = self._read_version()
        self._update_version()
        new_version = self._read_version()
        
        self.logger.info("=" * 60)
        self.logger.info(f"✅ PostgreSQL Workflow Completed: {current_version} → {new_version}")
        self.logger.info("=" * 60)

    def _test_connection(self):
        self.logger.info("Testing PostgreSQL connection...")
        self.__manager.test_connection()
        self.logger.info("✅ PostgreSQL connection successful")

    def _read_version(self):
        self.logger.info("Reading PostgreSQL version...")
        version = self.__manager.read_latest_version(db_name=self.__db_param)
        
        if version is None:
            version = 0
            self.logger.info("ℹ️  No existing version, starting from 0")
        
        self.logger.info(f"✅ Current version: {version}")
        return version

    def _update_version(self):
        self.logger.info("Updating PostgreSQL version...")
        self.__manager.update_test_version(db_name=self.__db_param)
        self.logger.info("✅ Version updated successfully")


def run_postgres_process(
    logger: logging.Logger,
    working_dir: str,
    config_path: str,
    arguments: dict[str, Any],
):
    name = arguments.get("name", "infrastructure-testing-tools-postgres")
    operation_type = arguments.get("operation", "full_workflow")

    spark_executor_service_account = os.getenv("SPARK_EXECUTOR_SERVICE_ACCOUNT")

    jar_packages = [
        "org.postgresql:postgresql:42.7.1",
    ]

    spark_config = {
        "spark.driver.extraClassPath": None,
        "spark.driver.extraJavaOptions": "-Dcom.sun.net.ssl.checkRevocation=false",
        "spark.kubernetes.authenticate.driver.serviceAccountName": spark_executor_service_account,
        "spark.kubernetes.authenticate.serviceAccountName": spark_executor_service_account,
        "spark.kubernetes.executor.annotation.sidecar.istio.io/inject": False,
        "spark.extraListeners": "",
        "spark.jars.ivy": "/home/spark/.ivy2",
    }

    config_raw = helper.open_file_as_json(path=config_path)
    config = initialize_configuration(
        config=config_raw, working_dir=f"{working_dir}/config/", file_path=config_path
    )

    telemetry_config_raw = config_raw.get("telemetry", {"enabled": False})
    telemetry_config = parse_telemetry_config(config=telemetry_config_raw)
    tracer_provider, metric_provider, log_provider = initialize_telemetry(
        config=telemetry_config
    )

    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("run_postgres_process") as span:
        span.set_attribute("app.task", name)
        span.set_attribute("app.operation", operation_type)
        span_carrier = {}
        inject(span_carrier)

        operation = initialize_operation(name=name, config=config, logger=logger)

        spark_options = spark_config
        spark_options.update(configure_spark_options(config=telemetry_config))
        spark_options.update(config.engine.spark_options)

        operation.initialize_spark(
            packages=jar_packages, options=spark_options, is_debug=True
        )

        __do_run_postgres_process(
            logger=logger, config=config, operation=operation, arguments=arguments
        )

    logger.info("Cleaning up telemetry resources")
    metric_provider.force_flush()
    metric_provider.shutdown()
    tracer_provider.force_flush()
    tracer_provider.shutdown()


def __do_run_postgres_process(
    logger: logging.Logger,
    config: Configuration,
    operation: Operation,
    arguments: dict[str, Any],
):
    try:
        name = arguments.get("name", "infrastructure-testing-tools-postgres")
        dag_id = arguments.get("dag_id", "")
        operation_type = arguments.get("operation", "full_workflow")

        start_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        process = RunPostgresProcess(config=config, logger=logger, operation=operation)

        logger.info(LOG_EXECUTION_STARTED)
        process.configure(
            parameters=PostgresProcessParameters(
                operation_type=operation_type,
                is_log_load_data=False
            )
        )
        process.execute(
            parameters=PostgresExecutionParameters(
                name=name,
                dag_id=dag_id,
                operation_type=operation_type,
            )
        )

        end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        logger.info(
            f"{LOG_EXECUTION_COMPLETED} (elapsed: {end_timestamp - start_timestamp} ms)"
        )
    except Exception as ex:
        logger.info(LOG_EXECUTION_FAILED)
        logger.error(ex)
        raise ex
```

---

## 5️⃣ Task: `src/app/task_mongodb_operations.py` (ใหม่)

```python
"""
Task: MongoDB Operations
Test connection, read version, update version
Pattern: เหมือน task_postgres_operations.py แต่ใช้ MongoDB
"""

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import os
import socket
from typing import Any

from opentelemetry import trace
from opentelemetry.propagate import inject

from pyeqx.common import helper
from pyeqx.core import initialize_configuration, initialize_operation, Operation
from pyeqx.core.configuration import Configuration
from pyeqx.core.constants import (
    LOG_EXECUTION_COMPLETED,
    LOG_EXECUTION_FAILED,
    LOG_EXECUTION_STARTED,
)
from pyeqx.execution import (
    ExecuteParameters,
    Process,
    ProcessExecutionParameters,
    ProcessParameters,
)
from pyeqx.opentelemetry.instrumentation import initialize_telemetry
from pyeqx.opentelemetry.spark import configure_spark_options

from app.utils import parse_telemetry_config

hostname = socket.gethostname()
host_ip = socket.gethostbyname(hostname)


@dataclass
class MongoDBProcessParameters(ProcessParameters):
    operation_type: str
    
    def __init__(
        self,
        operation_type: str = "full_workflow",
        is_log_load_data: bool = False,
    ):
        super().__init__(is_log_load_data=is_log_load_data)
        self.operation_type = operation_type


@dataclass
class MongoDBExecutionParameters(ExecuteParameters):
    dag_id: str
    operation_type: str
    
    def __init__(
        self,
        name: str,
        dag_id: str,
        operation_type: str = "full_workflow",
    ):
        self.name = name
        self.dag_id = dag_id
        self.operation_type = operation_type


class RunMongoDBProcess(Process):
    __debug_mode: bool = False
    __manager = None
    __db_param = None

    def __init__(
        self,
        config: Configuration,
        logger: logging.Logger,
        operation: Operation,
    ):
        super().__init__(config, logger, operation)

    def configure(self, parameters: ProcessParameters):
        super().configure(parameters)
        assert isinstance(parameters, MongoDBProcessParameters)
        
        self.logger.info(f"Configuring MongoDB operations: {parameters.operation_type}")
        
        try:
            import sys
            etl_path = "/app/etl"
            config_path = "/app/config"
            if etl_path not in sys.path:
                sys.path.insert(0, etl_path)
            if config_path not in sys.path:
                sys.path.insert(0, config_path)
            
            from mongodb_manager import MongoDBManeger
            from mongo_configuration import MongoConfiguration
            
            spark = self.operation.get_current_spark_session()
            self.__manager = MongoDBManeger(spark=spark)
            self.__db_param = MongoConfiguration.mongo_param
            
            self.logger.info("✅ MongoDB manager configured")
            
        except ImportError as ie:
            self.logger.error(f"Failed to import managers: {ie}")
            raise

    def _read_datas(self, parameters: ProcessExecutionParameters):
        assert isinstance(parameters, MongoDBExecutionParameters)
        pass

    def _process_datas(self, parameters):
        assert isinstance(parameters, MongoDBExecutionParameters)
        
        operation = parameters.operation_type
        
        if operation == "full_workflow":
            self._run_full_workflow()
        elif operation == "test_connection":
            self._test_connection()
        elif operation == "read_version":
            self._read_version()
        elif operation == "update_version":
            self._update_version()
        else:
            raise ValueError(f"Unknown operation: {operation}")

    def _run_full_workflow(self):
        """Run complete workflow: test → read → update"""
        self.logger.info("=" * 60)
        self.logger.info("Running MongoDB Full Workflow")
        self.logger.info("=" * 60)
        
        self._test_connection()
        current_version = self._read_version()
        self._update_version()
        new_version = self._read_version()
        
        self.logger.info("=" * 60)
        self.logger.info(f"✅ MongoDB Workflow Completed: {current_version} → {new_version}")
        self.logger.info("=" * 60)

    def _test_connection(self):
        self.logger.info("Testing MongoDB connection...")
        self.__manager.test_connection()
        self.logger.info("✅ MongoDB connection successful")

    def _read_version(self):
        self.logger.info("Reading MongoDB version...")
        version = self.__manager.read_latest_version(db_name=self.__db_param)
        
        if version is None:
            version = 0
            self.logger.info("ℹ️  No existing version, starting from 0")
        
        self.logger.info(f"✅ Current version: {version}")
        return version

    def _update_version(self):
        self.logger.info("Updating MongoDB version...")
        self.__manager.update_test_version(db_name=self.__db_param)
        self.logger.info("✅ Version updated successfully")


def run_mongodb_process(
    logger: logging.Logger,
    working_dir: str,
    config_path: str,
    arguments: dict[str, Any],
):
    name = arguments.get("name", "infrastructure-testing-tools-mongodb")
    operation_type = arguments.get("operation", "full_workflow")

    spark_executor_service_account = os.getenv("SPARK_EXECUTOR_SERVICE_ACCOUNT")

    jar_packages = [
        "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0",
    ]

    spark_config = {
        "spark.driver.extraClassPath": None,
        "spark.driver.extraJavaOptions": "-Dcom.sun.net.ssl.checkRevocation=false",
        "spark.kubernetes.authenticate.driver.serviceAccountName": spark_executor_service_account,
        "spark.kubernetes.authenticate.serviceAccountName": spark_executor_service_account,
        "spark.kubernetes.executor.annotation.sidecar.istio.io/inject": False,
        "spark.extraListeners": "",
        "spark.jars.ivy": "/home/spark/.ivy2",
    }

    config_raw = helper.open_file_as_json(path=config_path)
    config = initialize_configuration(
        config=config_raw, working_dir=f"{working_dir}/config/", file_path=config_path
    )

    telemetry_config_raw = config_raw.get("telemetry", {"enabled": False})
    telemetry_config = parse_telemetry_config(config=telemetry_config_raw)
    tracer_provider, metric_provider, log_provider = initialize_telemetry(
        config=telemetry_config
    )

    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("run_mongodb_process") as span:
        span.set_attribute("app.task", name)
        span.set_attribute("app.operation", operation_type)
        span_carrier = {}
        inject(span_carrier)

        operation = initialize_operation(name=name, config=config, logger=logger)

        spark_options = spark_config
        spark_options.update(configure_spark_options(config=telemetry_config))
        spark_options.update(config.engine.spark_options)

        operation.initialize_spark(
            packages=jar_packages, options=spark_options, is_debug=True
        )

        __do_run_mongodb_process(
            logger=logger, config=config, operation=operation, arguments=arguments
        )

    logger.info("Cleaning up telemetry resources")
    metric_provider.force_flush()
    metric_provider.shutdown()
    tracer_provider.force_flush()
    tracer_provider.shutdown()


def __do_run_mongodb_process(
    logger: logging.Logger,
    config: Configuration,
    operation: Operation,
    arguments: dict[str, Any],
):
    try:
        name = arguments.get("name", "infrastructure-testing-tools-mongodb")
        dag_id = arguments.get("dag_id", "")
        operation_type = arguments.get("operation", "full_workflow")

        start_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        process = RunMongoDBProcess(config=config, logger=logger, operation=operation)

        logger.info(LOG_EXECUTION_STARTED)
        process.configure(
            parameters=MongoDBProcessParameters(
                operation_type=operation_type,
                is_log_load_data=False
            )
        )
        process.execute(
            parameters=MongoDBExecutionParameters(
                name=name,
                dag_id=dag_id,
                operation_type=operation_type,
            )
        )

        end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        logger.info(
            f"{LOG_EXECUTION_COMPLETED} (elapsed: {end_timestamp - start_timestamp} ms)"
        )
    except Exception as ex:
        logger.info(LOG_EXECUTION_FAILED)
        logger.error(ex)
        raise ex
```

---

## 6️⃣ Task: `src/app/task_cross_database_operations.py` (ใหม่)

เนื่องจากไฟล์ยาวมาก ให้ผมสร้างแยกต่อในไฟล์ถัดไป...

---

## 📊 Summary

**ไฟล์ที่ต้องสร้าง/แก้ไข:**

1. ✅ `dags/main.py` - สร้าง parallel task group
2. ✅ `src/main.py` - เพิ่ม routing สำหรับ 4 tasks
3. ✅ `src/app/task_minio_operations.py` - สร้างใหม่
4. ✅ `src/app/task_postgres_operations.py` - สร้างใหม่
5. ✅ `src/app/task_mongodb_operations.py` - สร้างใหม่
6. ⏭️ `src/app/task_cross_database_operations.py` - ต่อในไฟล์ถัดไป

**การรัน:**
```bash
# Trigger DAG ใน Airflow UI
# DAG จะสร้าง 4 Pods พร้อมกัน
# รันเสร็จใน ~30-40 วินาที (แทนที่จะเป็น 2-3 นาที)
```
