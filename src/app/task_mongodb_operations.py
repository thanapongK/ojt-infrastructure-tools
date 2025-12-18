"""
Task: MongoDB Operations
Test connection, read version, update version
Pattern: Similar to task_postgres_operations.py but uses MongoDB
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
from config import MongoConfiguration
from helper import MongoDBManeger, StandardResult


@dataclass
class MongoDBProcessParameters(ProcessParameters):
    def __init__(self, is_log_load_data: bool = False):
        super().__init__(is_log_load_data=is_log_load_data)


@dataclass
class MongoDBExecutionParameters(ExecuteParameters):
    dag_id: str

    def __init__(
        self,
        name: str,
        dag_id: str,
    ):
        self.name = name
        self.dag_id = dag_id


class RunMongoDBProcess(Process):
    __debug_mode: bool = False
    __manager: MongoDBManeger = None
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

        try:
            spark = self.operation.get_current_spark_session()
            self.__manager = MongoDBManeger(spark=spark)
            self.__db_param = MongoConfiguration.mongo_param

        except Exception as e:
            StandardResult.error("Failed to configure MongoDB manager", error=e)

    def _read_datas(self, parameters: ProcessExecutionParameters):
        # TODO: implement pre-read if needed
        assert isinstance(parameters, MongoDBExecutionParameters)

    def _process_datas(self, parameters):
        assert isinstance(parameters, MongoDBExecutionParameters)

        try:
            self._ensure_connection()
            self._update_version()
            self._read_version()

        except Exception as e:
            StandardResult.error("MongoDB workflow failed", error=e)

    def _ensure_connection(self):
        try:
            self.__manager.ensure_connection()
        except Exception as e:
            StandardResult.error("MongoDB connection failed", error=e)

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


def run_mongodb_process(
    logger: logging.Logger,
    working_dir: str,
    config_path: str,
    arguments: dict[str, Any],
):
    name = arguments.get("name", "infrastructure-testing-tools-mongodb")

    spark_executor_service_account = os.getenv("SPARK_EXECUTOR_SERVICE_ACCOUNT")

    jar_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7",
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
    with tracer.start_as_current_span("run_mongodb_process") as span:
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

        start_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        process = RunMongoDBProcess(config=config, logger=logger, operation=operation)

        logger.info(LOG_EXECUTION_STARTED)
        process.configure(parameters=MongoDBProcessParameters(is_log_load_data=False))
        process.execute(parameters=MongoDBExecutionParameters(name=name, dag_id=dag_id))

        end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        logger.info(
            f"{LOG_EXECUTION_COMPLETED} (elapsed: {end_timestamp - start_timestamp} ms)"
        )
    except Exception as ex:
        logger.info(LOG_EXECUTION_FAILED)
        logger.error(ex)
        raise ex
