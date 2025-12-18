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
from config import (
    MinioConfiguration,
    MongoConfiguration,
    PostgresConfiguration,
)
from helper import (
    CrossDatabaseManager,
    MinioManager,
    MongoDBManeger,
    PostgresManager,
    StandardResult,
)


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
    __cross_db_manager: CrossDatabaseManager = None
    __minio_manager: MinioManager = None
    __postgres_manager: PostgresManager = None
    __mongodb_manager: MongoDBManeger = None
    __mongo_param: str = None
    __postgres_param: str = None
    __minio_param: str = None

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

            self.__cross_db_manager = CrossDatabaseManager()
            self.__minio_manager = MinioManager(spark=spark)
            self.__postgres_manager = PostgresManager(spark=spark)
            self.__mongodb_manager = MongoDBManeger(spark=spark)

            self.__mongo_param = MongoConfiguration.mongo_param
            self.__postgres_param = PostgresConfiguration.postgres_param
            self.__minio_param = MinioConfiguration.minio_param

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
            self.__cross_db_manager.cross_version_to(
                read=self.__mongo_param,
                write=self.__postgres_param,
            )

        except Exception as e:
            StandardResult.error("MongoDB → PostgreSQL failed", error=e)

    def _transfer_postgres_to_mongo(self):
        try:

            self.__cross_db_manager.cross_version_to(
                read=self.__postgres_param,
                write=self.__mongo_param,
            )

        except Exception as e:
            StandardResult.error("PostgreSQL → MongoDB failed", error=e)

    def _transfer_mongo_to_s3(self):
        try:
            result = self.__cross_db_manager.cross_data_to(
                read=self.__mongo_param,
                write=self.__minio_param,
                source_collection=MongoConfiguration.COLLECTION,
                target_path=MongoConfiguration.TRANSFER_TARGET_PATH,
                output_filename=MongoConfiguration.TRANSFER_OUTPUT_FILENAME,
                format=MinioConfiguration.DEFAULT_FORMAT,
                mode=MinioConfiguration.DEFAULT_MODE,
            )

            if result is None:
                StandardResult.error(
                    "No data transferred from MongoDB to S3", error=None
                )
                return

            StandardResult.show_log_info(
                [
                    f"Transfer completed: {result['row_count']} records",
                    f"{result['source']} → {result['target']} (delta format)",
                    "Path: ojt/mongodb",
                ],
                level="info",
                prefix="✅ ",
            )

        except Exception as e:
            StandardResult.error("MongoDB → S3 failed", error=e)

    def _transfer_s3_to_mongo(self):
        try:
            source_path = self.__minio_manager.build_s3_path(
                bucket=MinioConfiguration.MINIO_BUCKET,
                layer=MinioConfiguration.S3_LAYER_SILVER,
                filename=MinioConfiguration.RAW_VALID_DATA_USER,
                is_directory=True,
            )

            total_records, valid_records, invalid_records = (
                self.__cross_db_manager.transfer_validated_user_data(
                    write=self.__mongo_param,
                    source_path=source_path,
                    target_name=MongoConfiguration.USER_COLLECTION,
                    mode=MongoConfiguration.DEFAULT_MODE,
                )
            )

            StandardResult.show_log_info(
                [
                    "Transfer completed with validation:",
                    f"Total records: {total_records}",
                    f"Valid records: {valid_records}",
                    f"Invalid records: {invalid_records}",
                    "S3 (silver/raw_valid_data_user) → MongoDB (collection: user)",
                    "Mode: overwrite",
                ],
                level="info",
                prefix="✅ ",
            )

        except Exception as e:
            StandardResult.error("S3 → MongoDB failed", error=e)

    def _transfer_postgres_to_s3(self):
        try:
            result = self.__cross_db_manager.cross_data_to(
                read=self.__postgres_param,
                write=self.__minio_param,
                source_table=PostgresConfiguration.TABLE,
                target_path=PostgresConfiguration.TRANSFER_TARGET_PATH,
                output_filename=PostgresConfiguration.TRANSFER_OUTPUT_FILENAME,
                format=MinioConfiguration.DEFAULT_FORMAT,
                mode=MinioConfiguration.DEFAULT_MODE,
            )

            if result is None:
                StandardResult.error(
                    "No data transferred from PostgreSQL to S3", error=None
                )
                return

            StandardResult.show_log_info(
                [
                    f"Transfer completed: {result['row_count']} records",
                    f"{result['source']} → {result['target']} (delta format)",
                    "Path: ojt/postgresql",
                ],
                level="info",
                prefix="✅ ",
            )

        except Exception as e:
            StandardResult.error("PostgreSQL → S3 failed", error=e)

    def _transfer_s3_to_postgres(self):
        try:
            source_path = self.__minio_manager.build_s3_path(
                bucket=MinioConfiguration.MINIO_BUCKET,
                layer=MinioConfiguration.S3_LAYER_SILVER,
                filename=MinioConfiguration.RAW_VALID_DATA_USER,
                is_directory=True,
            )

            total_records, valid_records, invalid_records = (
                self.__cross_db_manager.transfer_validated_user_data(
                    write=self.__postgres_param,
                    source_path=source_path,
                    target_name=PostgresConfiguration.USER_TABLE,
                    mode=PostgresConfiguration.DEFAULT_MODE,
                )
            )

            StandardResult.show_log_info(
                [
                    "Transfer completed with validation:",
                    f"Total records: {total_records}",
                    f"Valid records: {valid_records}",
                    f"Invalid records: {invalid_records}",
                    "S3 (silver/raw_valid_data_user) → PostgreSQL (table: user)",
                    "Mode: overwrite",
                ],
                level="info",
                prefix="✅ ",
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
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",  # this version use with hadoop 3.3.4
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7",
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
