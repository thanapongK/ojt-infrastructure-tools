"""
Task: MinIO Operations
Upload, read, and list objects in MinIO/S3
"""

from dataclasses import dataclass
from datetime import datetime, timezone
import glob
import logging
import os
from typing import Any

from opentelemetry import trace
from opentelemetry.propagate import inject

from pyeqx.common import helper as pyeqx_helper
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
from helper.standard_result import StandardResult
from helper.minio_manager import MinioManager
from config.minio_configuration import MinioConfiguration


@dataclass
class MinioProcessParameters(ProcessParameters):
    def __init__(self, is_log_load_data: bool = False):
        super().__init__(is_log_load_data=is_log_load_data)


@dataclass
class MinioExecutionParameters(ExecuteParameters):
    dag_id: str

    def __init__(
        self,
        name: str,
        dag_id: str,
    ):
        self.name = name
        self.dag_id = dag_id


class RunMinioProcess(Process):
    __debug_mode: bool = False
    __manager: MinioManager = None

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

        try:
            spark = self.operation.get_current_spark_session()
            self.__manager = MinioManager(spark=spark)

        except Exception as e:
            StandardResult.error("Failed to configure MinIO manager", error=e)

    def _read_datas(self, parameters: ProcessExecutionParameters):
        # TODO: implement pre-read if needed
        assert isinstance(parameters, MinioExecutionParameters)

    def _process_datas(self, parameters):
        assert isinstance(parameters, MinioExecutionParameters)

        try:
            self._ensure_connection()

            csv_pattern = os.path.join(
                MinioConfiguration.DATA_DIR, MinioConfiguration.VALID_USERS_FILE
            )
            csv_files = glob.glob(csv_pattern)

            if csv_files:
                self._upload_data(
                    local_file_path=csv_files[0],
                    output_filename=MinioConfiguration.OUTPUT_CSV_DATA,
                )

            json_path = os.path.join(
                MinioConfiguration.DATA_DIR, MinioConfiguration.SIMPLE_JSON_FILE
            )
            if os.path.exists(json_path):
                self._upload_data(
                    local_file_path=json_path,
                    output_filename=MinioConfiguration.OUTPUT_JSON_DATA,
                )

            self._read_all_data()

        except Exception as e:
            StandardResult.error("MinIO workflow failed", error=e)

    def _ensure_connection(self):
        try:
            self.__manager.ensure_connection()
        except Exception as e:
            StandardResult.error("MinIO connection failed", error=e)

    def _upload_data(self, local_file_path: str, output_filename: str):
        try:
            self.__manager.write_data_to_s3(
                local_file_path=local_file_path,
                s3_path=MinioConfiguration.S3_LAYER_SILVER,
                output_filename=output_filename,
                format=MinioConfiguration.DEFAULT_FORMAT,
                mode=MinioConfiguration.DEFAULT_MODE,
            )

        except Exception as e:
            StandardResult.error(f"Upload failed: {local_file_path}", error=e)

    def _read_all_data(self):
        try:
            # Build S3 paths dynamically from components
            csv_path = self.__manager.build_s3_path(
                bucket=MinioConfiguration.MINIO_BUCKET,
                layer=MinioConfiguration.S3_LAYER_SILVER,
                filename=MinioConfiguration.OUTPUT_CSV_DATA,
                is_directory=True,
            )
            csv_df = self.__manager.read_data_from_s3(
                path=csv_path, format=MinioConfiguration.DEFAULT_FORMAT
            )

            json_path = self.__manager.build_s3_path(
                bucket=MinioConfiguration.MINIO_BUCKET,
                layer=MinioConfiguration.S3_LAYER_SILVER,
                filename=MinioConfiguration.OUTPUT_JSON_DATA,
                is_directory=True,
            )
            json_df = self.__manager.read_data_from_s3(
                path=json_path, format=MinioConfiguration.DEFAULT_FORMAT
            )

        except Exception as e:
            StandardResult.error("Read data failed", error=e)


def run_minio_process(
    logger: logging.Logger,
    working_dir: str,
    config_path: str,
    arguments: dict[str, Any],
):
    name = arguments.get("name", "infrastructure-testing-tools-minio")

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

    config_raw = pyeqx_helper.open_file_as_json(path=config_path)
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

        start_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        process = RunMinioProcess(config=config, logger=logger, operation=operation)

        logger.info(LOG_EXECUTION_STARTED)
        process.configure(parameters=MinioProcessParameters(is_log_load_data=False))
        process.execute(parameters=MinioExecutionParameters(name=name, dag_id=dag_id))

        end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        logger.info(
            f"{LOG_EXECUTION_COMPLETED} (elapsed: {end_timestamp - start_timestamp} ms)"
        )
    except Exception as ex:
        logger.info(LOG_EXECUTION_FAILED)
        logger.error(ex)
        raise ex
