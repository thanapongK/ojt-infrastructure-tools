"""
Task: Cross-Database Operations
Bidirectional data transfer between MongoDB, PostgreSQL, and MinIO/S3
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
from helper.minio_manager import MinioManager
from helper.postgres_manager import PostgresManager
from helper.mongodb_manager import MongoDBManeger
from config.mongo_configuration import MongoConfiguration
from config.postgres_configuration import PostgresConfiguration
from models.schemas import get_raw_valid_data_user_schema


@dataclass
class CrossDatabaseProcessParameters(ProcessParameters):
    def __init__(self, is_log_load_data: bool = False):
        super().__init__(is_log_load_data=is_log_load_data)


@dataclass
class CrossDatabaseExecutionParameters(ExecuteParameters):
    dag_id: str

    def __init__(
        self,
        name: str,
        dag_id: str,
    ):
        self.name = name
        self.dag_id = dag_id


class RunCrossDatabaseProcess(Process):
    """
    Process for Cross-Database Operations
    Supports 6 operations:
    1. MongoDB → PostgreSQL
    2. PostgreSQL → MongoDB
    3. MongoDB → S3
    4. S3 → MongoDB
    5. PostgreSQL → S3
    6. S3 → PostgreSQL
    """

    __debug_mode: bool = False
    __minio_manager: MinioManager = None
    __postgres_manager: PostgresManager = None
    __mongodb_manager: MongoDBManeger = None
    __mongo_param = None
    __postgres_param = None

    def __init__(
        self,
        config: Configuration,
        logger: logging.Logger,
        operation: Operation,
    ):
        super().__init__(config, logger, operation)

    def configure(self, parameters: ProcessParameters):
        super().configure(parameters)
        assert isinstance(parameters, CrossDatabaseProcessParameters)

        try:
            spark = self.operation.get_current_spark_session()

            self.__minio_manager = MinioManager(spark=spark)
            self.__postgres_manager = PostgresManager(spark=spark)
            self.__mongodb_manager = MongoDBManeger(spark=spark)

            self.__mongo_param = MongoConfiguration.mongo_param
            self.__postgres_param = PostgresConfiguration.postgres_param
            self.__schema = get_raw_valid_data_user_schema

        except Exception as e:
            StandardResult.error("Failed to configure cross-database managers", error=e)

    def _read_datas(self, parameters: ProcessExecutionParameters):
        # TODO: implement pre-read if needed
        assert isinstance(parameters, CrossDatabaseExecutionParameters)

    def _process_datas(self, parameters):
        assert isinstance(parameters, CrossDatabaseExecutionParameters)

        try:
            self._transfer_mongo_to_postgres()
            self._transfer_postgres_to_mongo()
            self._transfer_mongo_to_s3()
            self._transfer_s3_to_mongo()
            self._transfer_postgres_to_s3()
            self._transfer_s3_to_postgres()

        except Exception as e:
            StandardResult.error("Cross-database transfers failed", error=e)

    def _transfer_mongo_to_postgres(self):
        try:
            mongo_df = self.__mongodb_manager.read_data(db_name=self.__mongo_param)
            self.__postgres_manager.write_data(
                df=mongo_df,
                db_name=self.__postgres_param,
                table_name=PostgresConfiguration.TABLE,
                mode="overwrite",
            )

        except Exception as e:
            StandardResult.error("MongoDB → PostgreSQL failed", error=e)

    def _transfer_postgres_to_mongo(self):
        try:
            pg_df = self.__postgres_manager.read_data(
                db_name=self.__postgres_param, table_name=PostgresConfiguration.TABLE
            )

            self.__mongodb_manager.write_data(
                df=pg_df,
                db_name=self.__mongo_param,
                collection_name=MongoConfiguration.COLLECTION,
                mode="overwrite",
            )

        except Exception as e:
            StandardResult.error("PostgreSQL → MongoDB failed", error=e)

    def _transfer_mongo_to_s3(self):
        try:
            mongo_df = self.__mongodb_manager.read_data(db_name=self.__mongo_param)

            self.__minio_manager.write_data_to_s3(
                data=mongo_df,
                s3_path="silver",
                output_filename="mongo_to_s3_transfer",
                format="delta",
                mode="overwrite",
            )

        except Exception as e:
            StandardResult.error("MongoDB → S3 failed", error=e)

    def _transfer_s3_to_mongo(self):
        try:
            s3_path = "s3a://ojtbucket/silver/mongo_to_s3_transfer"
            s3_df = self.__minio_manager.read_data_from_s3(path=s3_path, format="delta")

            self.__mongodb_manager.write_data(
                df=s3_df,
                db_name=self.__mongo_param,
                collection_name=MongoConfiguration.COLLECTION,
                mode="overwrite",
            )

        except Exception as e:
            StandardResult.error("S3 → MongoDB failed", error=e)

    def _transfer_postgres_to_s3(self):
        try:
            pg_df = self.__postgres_manager.read_data(
                db_name=self.__postgres_param, table_name="mongo_to_postgres_transfer"
            )

            self.__minio_manager.write_data_to_s3(
                data=pg_df,
                s3_path="silver",
                output_filename="postgres_to_s3_transfer",
                format="delta",
                mode="overwrite",
            )

        except Exception as e:
            StandardResult.error("PostgreSQL → S3 failed", error=e)

    def _transfer_s3_to_postgres(self):
        try:
            s3_path = "s3a://ojtbucket/silver/postgres_to_s3_transfer"
            s3_df = self.__minio_manager.read_data_from_s3(path=s3_path, format="delta")

            self.__postgres_manager.write_data(
                df=s3_df,
                db_name=self.__postgres_param,
                table_name="s3_to_postgres_transfer",
                mode="overwrite",
            )

        except Exception as e:
            StandardResult.error("S3 → PostgreSQL failed", error=e)


def run_cross_database_process(
    logger: logging.Logger,
    working_dir: str,
    config_path: str,
    arguments: dict[str, Any],
):
    name = arguments.get("name", "infrastructure-testing-tools-cross-database")

    spark_executor_service_account = os.getenv("SPARK_EXECUTOR_SERVICE_ACCOUNT")

    # Combined JAR packages (Delta, MongoDB, PostgreSQL)
    jar_packages = [
        "io.delta:delta-spark_2.12:3.3.2",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0",
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
    with tracer.start_as_current_span("run_cross_database_process") as span:
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

        __do_run_cross_database_process(
            logger=logger, config=config, operation=operation, arguments=arguments
        )

    logger.info("Cleaning up telemetry resources")
    metric_provider.force_flush()
    metric_provider.shutdown()
    tracer_provider.force_flush()
    tracer_provider.shutdown()


def __do_run_cross_database_process(
    logger: logging.Logger,
    config: Configuration,
    operation: Operation,
    arguments: dict[str, Any],
):
    try:
        name = arguments.get("name", "infrastructure-testing-tools-cross-database")
        dag_id = arguments.get("dag_id", "")

        start_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        process = RunCrossDatabaseProcess(
            config=config, logger=logger, operation=operation
        )

        logger.info(LOG_EXECUTION_STARTED)
        process.configure(
            parameters=CrossDatabaseProcessParameters(is_log_load_data=False)
        )
        process.execute(
            parameters=CrossDatabaseExecutionParameters(name=name, dag_id=dag_id)
        )

        end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        logger.info(
            f"{LOG_EXECUTION_COMPLETED} (elapsed: {end_timestamp - start_timestamp} ms)"
        )
    except Exception as ex:
        logger.info(LOG_EXECUTION_FAILED)
        logger.error(ex)
        raise ex
