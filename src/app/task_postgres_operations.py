"""
Task: PostgreSQL Operations
Test connection, read version, update version
Pattern: อิงจาก task_spark_process.py
"""

from dataclasses import dataclass
from datetime import datetime, timezone
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
from helper.postgres_manager import PostgresManager
from config.postgres_configuration import PostgresConfiguration


@dataclass
class PostgresProcessParameters(ProcessParameters):
    def __init__(self, is_log_load_data: bool = False):
        super().__init__(is_log_load_data=is_log_load_data)


@dataclass
class PostgresExecutionParameters(ExecuteParameters):
    dag_id: str

    def __init__(
        self,
        name: str,
        dag_id: str,
    ):
        self.name = name
        self.dag_id = dag_id


class RunPostgresProcess(Process):
    __debug_mode: bool = False
    __manager: PostgresManager = None
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

        try:
            spark = self.operation.get_current_spark_session()
            self.__manager = PostgresManager(spark=spark)
            self.__db_param = PostgresConfiguration.postgres_param

        except Exception as e:
            StandardResult.error("Failed to configure PostgreSQL manager", error=e)

    def _read_datas(self, parameters: ProcessExecutionParameters):
        # TODO: implement pre-read if needed
        assert isinstance(parameters, PostgresExecutionParameters)

    def _process_datas(self, parameters):
        assert isinstance(parameters, PostgresExecutionParameters)

        try:
            self._test_connection()
            self._update_version()
            self._read_version()

        except Exception as e:
            StandardResult.error("PostgreSQL workflow failed", error=e)

    def _test_connection(self):
        try:
            self.__manager.test_connection()
        except Exception as e:
            StandardResult.error("PostgreSQL connection failed", error=e)

    def _read_version(self) -> float:
        try:
            version = self.__manager.read_latest_version(db_name=self.__db_param)

            if version is None:
                version = 0

            return version
        except Exception as e:
            StandardResult.error("Read version failed", error=e)

    def _update_version(self):
        try:
            self.__manager.update_test_version(db_name=self.__db_param)
        except Exception as e:
            StandardResult.error("Update version failed", error=e)


def run_postgres_process(
    logger: logging.Logger,
    working_dir: str,
    config_path: str,
    arguments: dict[str, Any],
):
    name = arguments.get("name", "infrastructure-testing-tools-postgres")

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
    with tracer.start_as_current_span("run_postgres_process") as span:
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

        start_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        process = RunPostgresProcess(config=config, logger=logger, operation=operation)

        logger.info(LOG_EXECUTION_STARTED)
        process.configure(parameters=PostgresProcessParameters(is_log_load_data=False))
        process.execute(
            parameters=PostgresExecutionParameters(name=name, dag_id=dag_id)
        )

        end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        logger.info(
            f"{LOG_EXECUTION_COMPLETED} (elapsed: {end_timestamp - start_timestamp} ms)"
        )
    except Exception as ex:
        logger.info(LOG_EXECUTION_FAILED)
        logger.error(ex)
        raise ex
