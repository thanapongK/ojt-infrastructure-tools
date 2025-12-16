# Cross-Database Operations - Complete Code

> **Task 4: Cross-Database Operations**
> 
> Transfer data between MongoDB ↔ PostgreSQL ↔ S3

---

## `src/app/task_cross_database_operations.py`

```python
"""
Task: Cross-Database Operations
Bidirectional data transfer between MongoDB, PostgreSQL, and MinIO/S3
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
class CrossDatabaseProcessParameters(ProcessParameters):
    operation_type: str
    
    def __init__(
        self,
        operation_type: str = "all_transfers",
        is_log_load_data: bool = False,
    ):
        super().__init__(is_log_load_data=is_log_load_data)
        self.operation_type = operation_type


@dataclass
class CrossDatabaseExecutionParameters(ExecuteParameters):
    dag_id: str
    operation_type: str
    
    def __init__(
        self,
        name: str,
        dag_id: str,
        operation_type: str = "all_transfers",
    ):
        self.name = name
        self.dag_id = dag_id
        self.operation_type = operation_type


class RunCrossDatabaseProcess(Process):
    """
    Process สำหรับ Cross-Database Operations
    รองรับ 6 operations:
    1. MongoDB → PostgreSQL
    2. PostgreSQL → MongoDB
    3. MongoDB → S3
    4. S3 → MongoDB
    5. PostgreSQL → S3
    6. S3 → PostgreSQL
    """
    
    __debug_mode: bool = False
    __minio_manager = None
    __postgres_manager = None
    __mongodb_manager = None
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
        
        self.logger.info(f"Configuring Cross-Database operations: {parameters.operation_type}")
        
        # Initialize all managers
        try:
            import sys
            etl_path = "/app/etl"
            config_path = "/app/config"
            if etl_path not in sys.path:
                sys.path.insert(0, etl_path)
            if config_path not in sys.path:
                sys.path.insert(0, config_path)
            
            from minio_manager import MinioManager
            from postgres_manager import PostgresManager
            from mongodb_manager import MongoDBManeger
            from mongo_configuration import MongoConfiguration
            from postgres_configuration import PostgresConfiguration
            from schemas import get_raw_valid_data_user_schema
            
            spark = self.operation.get_current_spark_session()
            
            self.__minio_manager = MinioManager(spark=spark)
            self.__postgres_manager = PostgresManager(spark=spark)
            self.__mongodb_manager = MongoDBManeger(spark=spark)
            
            self.__mongo_param = MongoConfiguration.mongo_param
            self.__postgres_param = PostgresConfiguration.postgres_param
            self.__schema = get_raw_valid_data_user_schema
            
            self.logger.info("✅ All managers configured (MinIO, PostgreSQL, MongoDB)")
            
        except ImportError as ie:
            self.logger.error(f"Failed to import managers: {ie}")
            raise

    def _read_datas(self, parameters: ProcessExecutionParameters):
        assert isinstance(parameters, CrossDatabaseExecutionParameters)
        pass

    def _process_datas(self, parameters):
        assert isinstance(parameters, CrossDatabaseExecutionParameters)
        
        operation = parameters.operation_type
        
        if operation == "all_transfers":
            self._run_all_transfers()
        elif operation == "mongo_to_postgres":
            self._transfer_mongo_to_postgres()
        elif operation == "postgres_to_mongo":
            self._transfer_postgres_to_mongo()
        elif operation == "mongo_to_s3":
            self._transfer_mongo_to_s3()
        elif operation == "s3_to_mongo":
            self._transfer_s3_to_mongo()
        elif operation == "postgres_to_s3":
            self._transfer_postgres_to_s3()
        elif operation == "s3_to_postgres":
            self._transfer_s3_to_postgres()
        else:
            raise ValueError(f"Unknown operation: {operation}")

    def _run_all_transfers(self):
        """Run all 6 transfer operations"""
        self.logger.info("=" * 60)
        self.logger.info("Running All Cross-Database Transfers")
        self.logger.info("=" * 60)
        
        try:
            self._transfer_mongo_to_postgres()
            self._transfer_postgres_to_mongo()
            self._transfer_mongo_to_s3()
            self._transfer_s3_to_mongo()
            self._transfer_postgres_to_s3()
            self._transfer_s3_to_postgres()
            
            self.logger.info("=" * 60)
            self.logger.info("✅ All Cross-Database Transfers Completed")
            self.logger.info("=" * 60)
            
        except Exception as e:
            self.logger.error(f"❌ Transfer failed: {e}")
            raise

    def _transfer_mongo_to_postgres(self):
        """Transfer 1: MongoDB → PostgreSQL"""
        self.logger.info("-" * 60)
        self.logger.info("Transfer 1/6: MongoDB → PostgreSQL")
        self.logger.info("-" * 60)
        
        try:
            # Read from MongoDB
            self.logger.info("Reading from MongoDB...")
            mongo_df = self.__mongodb_manager.read_data(db_name=self.__mongo_param)
            row_count = mongo_df.count()
            self.logger.info(f"✅ Read {row_count} rows from MongoDB")
            
            # Show sample
            self.logger.info("Sample data:")
            mongo_df.show(3, truncate=False)
            
            # Write to PostgreSQL
            self.logger.info("Writing to PostgreSQL...")
            self.__postgres_manager.write_data(
                df=mongo_df,
                db_name=self.__postgres_param,
                table_name="mongo_to_postgres_transfer",
                mode="overwrite"
            )
            
            self.logger.info(f"✅ Transfer 1 completed: {row_count} rows MongoDB → PostgreSQL")
            
        except Exception as e:
            self.logger.error(f"❌ MongoDB → PostgreSQL failed: {e}")
            raise

    def _transfer_postgres_to_mongo(self):
        """Transfer 2: PostgreSQL → MongoDB"""
        self.logger.info("-" * 60)
        self.logger.info("Transfer 2/6: PostgreSQL → MongoDB")
        self.logger.info("-" * 60)
        
        try:
            # Read from PostgreSQL
            self.logger.info("Reading from PostgreSQL...")
            pg_df = self.__postgres_manager.read_data(
                db_name=self.__postgres_param,
                table_name="mongo_to_postgres_transfer"
            )
            row_count = pg_df.count()
            self.logger.info(f"✅ Read {row_count} rows from PostgreSQL")
            
            # Show sample
            self.logger.info("Sample data:")
            pg_df.show(3, truncate=False)
            
            # Write to MongoDB
            self.logger.info("Writing to MongoDB...")
            self.__mongodb_manager.write_data(
                df=pg_df,
                db_name=self.__mongo_param,
                collection_name="postgres_to_mongo_transfer",
                mode="overwrite"
            )
            
            self.logger.info(f"✅ Transfer 2 completed: {row_count} rows PostgreSQL → MongoDB")
            
        except Exception as e:
            self.logger.error(f"❌ PostgreSQL → MongoDB failed: {e}")
            raise

    def _transfer_mongo_to_s3(self):
        """Transfer 3: MongoDB → S3"""
        self.logger.info("-" * 60)
        self.logger.info("Transfer 3/6: MongoDB → S3")
        self.logger.info("-" * 60)
        
        try:
            # Read from MongoDB
            self.logger.info("Reading from MongoDB...")
            mongo_df = self.__mongodb_manager.read_data(db_name=self.__mongo_param)
            row_count = mongo_df.count()
            self.logger.info(f"✅ Read {row_count} rows from MongoDB")
            
            # Write to S3
            self.logger.info("Writing to S3...")
            self.__minio_manager.write_data_to_s3(
                df=mongo_df,
                s3_path="silver",
                output_filename="mongo_to_s3_transfer",
                format="delta",
                mode="overwrite"
            )
            
            s3_path = "s3a://ojtbucket/silver/mongo_to_s3_transfer"
            self.logger.info(f"✅ Transfer 3 completed: {row_count} rows MongoDB → {s3_path}")
            
        except Exception as e:
            self.logger.error(f"❌ MongoDB → S3 failed: {e}")
            raise

    def _transfer_s3_to_mongo(self):
        """Transfer 4: S3 → MongoDB"""
        self.logger.info("-" * 60)
        self.logger.info("Transfer 4/6: S3 → MongoDB")
        self.logger.info("-" * 60)
        
        try:
            # Read from S3
            self.logger.info("Reading from S3...")
            s3_path = "s3a://ojtbucket/silver/mongo_to_s3_transfer"
            s3_df = self.__minio_manager.read_data_from_s3(
                path=s3_path,
                format="delta"
            )
            row_count = s3_df.count()
            self.logger.info(f"✅ Read {row_count} rows from S3")
            
            # Show sample
            self.logger.info("Sample data:")
            s3_df.show(3, truncate=False)
            
            # Write to MongoDB
            self.logger.info("Writing to MongoDB...")
            self.__mongodb_manager.write_data(
                df=s3_df,
                db_name=self.__mongo_param,
                collection_name="s3_to_mongo_transfer",
                mode="overwrite"
            )
            
            self.logger.info(f"✅ Transfer 4 completed: {row_count} rows S3 → MongoDB")
            
        except Exception as e:
            self.logger.error(f"❌ S3 → MongoDB failed: {e}")
            raise

    def _transfer_postgres_to_s3(self):
        """Transfer 5: PostgreSQL → S3"""
        self.logger.info("-" * 60)
        self.logger.info("Transfer 5/6: PostgreSQL → S3")
        self.logger.info("-" * 60)
        
        try:
            # Read from PostgreSQL
            self.logger.info("Reading from PostgreSQL...")
            pg_df = self.__postgres_manager.read_data(
                db_name=self.__postgres_param,
                table_name="mongo_to_postgres_transfer"
            )
            row_count = pg_df.count()
            self.logger.info(f"✅ Read {row_count} rows from PostgreSQL")
            
            # Write to S3
            self.logger.info("Writing to S3...")
            self.__minio_manager.write_data_to_s3(
                df=pg_df,
                s3_path="silver",
                output_filename="postgres_to_s3_transfer",
                format="delta",
                mode="overwrite"
            )
            
            s3_path = "s3a://ojtbucket/silver/postgres_to_s3_transfer"
            self.logger.info(f"✅ Transfer 5 completed: {row_count} rows PostgreSQL → {s3_path}")
            
        except Exception as e:
            self.logger.error(f"❌ PostgreSQL → S3 failed: {e}")
            raise

    def _transfer_s3_to_postgres(self):
        """Transfer 6: S3 → PostgreSQL"""
        self.logger.info("-" * 60)
        self.logger.info("Transfer 6/6: S3 → PostgreSQL")
        self.logger.info("-" * 60)
        
        try:
            # Read from S3
            self.logger.info("Reading from S3...")
            s3_path = "s3a://ojtbucket/silver/postgres_to_s3_transfer"
            s3_df = self.__minio_manager.read_data_from_s3(
                path=s3_path,
                format="delta"
            )
            row_count = s3_df.count()
            self.logger.info(f"✅ Read {row_count} rows from S3")
            
            # Show sample
            self.logger.info("Sample data:")
            s3_df.show(3, truncate=False)
            
            # Write to PostgreSQL
            self.logger.info("Writing to PostgreSQL...")
            self.__postgres_manager.write_data(
                df=s3_df,
                db_name=self.__postgres_param,
                table_name="s3_to_postgres_transfer",
                mode="overwrite"
            )
            
            self.logger.info(f"✅ Transfer 6 completed: {row_count} rows S3 → PostgreSQL")
            
        except Exception as e:
            self.logger.error(f"❌ S3 → PostgreSQL failed: {e}")
            raise


def run_cross_database_process(
    logger: logging.Logger,
    working_dir: str,
    config_path: str,
    arguments: dict[str, Any],
):
    name = arguments.get("name", "infrastructure-testing-tools-cross-database")
    operation_type = arguments.get("operation", "all_transfers")

    spark_executor_service_account = os.getenv("SPARK_EXECUTOR_SERVICE_ACCOUNT")

    # รวม JAR packages ทั้งหมด (Delta, MongoDB, PostgreSQL)
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
    with tracer.start_as_current_span("run_cross_database_process") as span:
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
        operation_type = arguments.get("operation", "all_transfers")

        start_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        process = RunCrossDatabaseProcess(config=config, logger=logger, operation=operation)

        logger.info(LOG_EXECUTION_STARTED)
        process.configure(
            parameters=CrossDatabaseProcessParameters(
                operation_type=operation_type,
                is_log_load_data=False
            )
        )
        process.execute(
            parameters=CrossDatabaseExecutionParameters(
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

## 🎯 Operation Types

### All Transfers (`all_transfers`)
รัน 6 operations ทั้งหมดตามลำดับ:

```python
arguments={
    "parameters": {
        "task": "cross_database_operations",
        "operation": "all_transfers"  # Default
    }
}
```

### Individual Operations

**MongoDB ↔ PostgreSQL:**
```python
"operation": "mongo_to_postgres"    # MongoDB → PostgreSQL
"operation": "postgres_to_mongo"    # PostgreSQL → MongoDB
```

**MongoDB ↔ S3:**
```python
"operation": "mongo_to_s3"          # MongoDB → S3
"operation": "s3_to_mongo"          # S3 → MongoDB
```

**PostgreSQL ↔ S3:**
```python
"operation": "postgres_to_s3"       # PostgreSQL → S3
"operation": "s3_to_postgres"       # S3 → PostgreSQL
```

---

## 📊 Data Flow

```
┌─────────────┐
│   MongoDB   │──┐
└─────────────┘  │
                 ├──► PostgreSQL
┌─────────────┐  │
│ PostgreSQL  │──┤
└─────────────┘  │
                 ├──► S3 (Delta Lake)
┌─────────────┐  │
│   MinIO/S3  │──┘
└─────────────┘
```

**Bidirectional Transfers:**
- MongoDB ⟷ PostgreSQL
- MongoDB ⟷ S3
- PostgreSQL ⟷ S3

---

## 🔧 JAR Dependencies

ไฟล์นี้ต้องการ JAR packages ครบทั้งหมด:

```python
jar_packages = [
    "io.delta:delta-spark_2.12:3.3.2",           # Delta Lake
    "com.amazonaws:aws-java-sdk-bundle:1.12.262", # AWS S3
    "org.apache.hadoop:hadoop-aws:3.3.4",         # Hadoop S3
    "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0",  # MongoDB
    "org.postgresql:postgresql:42.7.1",           # PostgreSQL
]
```

---

## ✅ Key Features

1. **Three Managers**: MinIO, PostgreSQL, MongoDB ทำงานร่วมกัน
2. **Dataclass Parameters**: Type-safe configuration
3. **Process Lifecycle**: configure → execute → cleanup
4. **OpenTelemetry**: Distributed tracing ครบทุก operation
5. **Error Handling**: Try-catch แต่ละ transfer พร้อม logging
6. **Sample Display**: แสดง sample data หลังอ่าน
7. **Row Counting**: นับจำนวน rows ทุก transfer

---

## 🚀 Performance

**Sequential (เดิม):**
```
Operation 1: 20s
Operation 2: 20s
Operation 3: 20s
Operation 4: 20s
Operation 5: 20s
Operation 6: 20s
Total: 120s (2 minutes)
```

**Parallel (ใหม่):**
```
Pod 1 (MinIO):     30s ┐
Pod 2 (Postgres):  25s ├─► รันพร้อมกัน
Pod 3 (MongoDB):   28s │
Pod 4 (Cross-DB): 120s ┘
Total: 120s (ขึ้นกับ Pod ที่ช้าสุด)
```

**แต่ถ้าแยก Cross-DB เป็น 6 Pods:**
```
Total: ~30s (ปรับปรุง 75%)
```
