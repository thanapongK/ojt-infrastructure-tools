"""MinIO (S3-compatible) Configuration"""

import os
from pyeqx.core.configuration import Configuration


class MinioConfiguration:
    # MinIO connection settings
    MINIO_ENDPOINT = "http://localhost:9000"
    MINIO_ACCESS_KEY = "admin"
    MINIO_SECRET_KEY = "dWeXi9sj86"
    MINIO_BUCKET = "ojt-testing-tools"
    MINIO_PATH = "data"
    TABLE = "db_connect"
    S3A_PREFIX = "s3a://"

    # Local file paths
    DATA_DIR = "/opt/airflow/ojt/data/processed"
    VALID_USERS_FILE = "valid_users.csv"
    SIMPLE_JSON_FILE = "simple_json.json"

    # S3 data layers
    S3_LAYER_BRONZE = "bronze"
    S3_LAYER_SILVER = "silver"
    S3_LAYER_GOLD = "gold"

    # Output filenames in S3
    OUTPUT_CSV_DATA = "raw_valid_user_csv_data"
    OUTPUT_JSON_DATA = "simple_json_data"

    # Cross-database transfer filenames
    RAW_VALID_DATA_USER = "raw_valid_data_user"

    # Write configuration
    DEFAULT_FORMAT = "delta"
    DEFAULT_MODE = "overwrite"

    # Storage parameter for method calls
    minio_param = "s3"

    @classmethod
    def get_minio_endpoint(cls) -> str:
        """Get MinIO endpoint based on environment (Docker or local)"""
        if os.path.exists("/.dockerenv") or os.environ.get("AIRFLOW_HOME"):
            return "http://minio:9000"  # Use service name inside Docker network
        return cls.MINIO_ENDPOINT

    @classmethod
    def get_minio_host(cls) -> str:
        """Get MinIO host without http:// prefix for minio client"""
        endpoint = cls.get_minio_endpoint()
        return endpoint.replace("http://", "").replace("https://", "")

    @classmethod
    def get_s3a_uri(cls) -> str:
        """Return S3A URI for Spark"""
        return f"s3a://{cls.MINIO_BUCKET}/{cls.MINIO_PATH}"

    @classmethod
    def get_table_path(cls) -> str:
        """Return full path to table in MinIO"""
        return f"{cls.get_s3a_uri()}/{cls.TABLE}"

    @classmethod
    def get_minio_config(cls) -> dict:
        return {
            "endpoint": cls.get_minio_endpoint(),
            "access_key": cls.MINIO_ACCESS_KEY,
            "secret_key": cls.MINIO_SECRET_KEY,
            "bucket": cls.MINIO_BUCKET,
            "path": cls.MINIO_PATH,
            "table": cls.TABLE,
            "s3a_uri": cls.get_s3a_uri(),
            "table_path": cls.get_table_path(),
        }

    @classmethod
    def configure_s3a_hadoop(cls, hadoop_conf):
        """Configure Hadoop S3A settings for Spark"""
        endpoint = cls.get_minio_endpoint()
        hadoop_conf.set("fs.s3a.endpoint", endpoint)
        hadoop_conf.set("fs.s3a.access.key", cls.MINIO_ACCESS_KEY)
        hadoop_conf.set("fs.s3a.secret.key", cls.MINIO_SECRET_KEY)
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

    # NOTE: get_pyeqx_config() is deprecated - use config.json instead
    # This method created hardcoded local Spark configuration
    # All configuration should now come from config.json
    """
    @classmethod
    def get_pyeqx_config(cls) -> Configuration:
        """Create PyEQX configuration for MinIO"""

        config_dict = {
            "engine": {
                "storage": "system",
                "tmpPath": "/tmp/ojt_minio/",
                "isDedicatedSpark": True,
                "sparkExecutorCore": 1,
                "sparkExecutorMemory": "1g",
                "sparkEndpoint": "local[*]",
                "isDynamicAllocation": False,
                "sparkExecutorMinInstances": 1,
                "sparkExecutorMaxInstances": 1,
                "sparkOptions": {
                    "spark.master": "local[*]",
                    "spark.submit.deployMode": "client",
                    "spark.driver.host": "localhost",
                    "spark.driver.bindAddress": "0.0.0.0",
                    "spark.driver.memory": "1g",
                    "spark.executor.memory": "1g",
                    "spark.sql.shuffle.partitions": "4",
                    "spark.default.parallelism": "4",
                    "spark.kubernetes.executor.request.cores": "false",
                    "spark.kubernetes.allocation.batch.size": "0",
                    "spark.dynamicAllocation.enabled": "false",
                    "spark.scheduler.mode": "FIFO",
                    # S3A Configuration
                    "spark.hadoop.fs.s3a.endpoint": cls.get_minio_endpoint(),
                    "spark.hadoop.fs.s3a.access.key": cls.MINIO_ACCESS_KEY,
                    "spark.hadoop.fs.s3a.secret.key": cls.MINIO_SECRET_KEY,
                    "spark.hadoop.fs.s3a.path.style.access": "true",
                    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                    # Connection timeout settings
                    "spark.hadoop.fs.s3a.connection.timeout": "30000",  # 30s
                    "spark.hadoop.fs.s3a.connection.establish.timeout": "10000",  # 10s
                    "spark.hadoop.fs.s3a.attempts.maximum": "3",  # Retry 3 times
                    "spark.hadoop.fs.s3a.retry.limit": "3",
                    "spark.hadoop.fs.s3a.retry.interval": "500ms",
                    "spark.hadoop.fs.s3a.socket.recv.buffer": "65536",  # 64KB
                    "spark.hadoop.fs.s3a.socket.send.buffer": "65536",  # 64KB
                    # Performance tuning
                    "spark.hadoop.fs.s3a.connection.maximum": "50",
                    "spark.hadoop.fs.s3a.threads.max": "10",
                    "spark.hadoop.fs.s3a.fast.upload": "true",
                    "spark.hadoop.fs.s3a.fast.upload.buffer": "bytebuffer",
                    "spark.hadoop.fs.s3a.block.size": "33554432",  # 32MB
                    # Reduce logging noise
                    "spark.hadoop.fs.s3a.metrics.enabled": "false",
                    "spark.ui.showConsoleProgress": "false",
                },
            },
            "app": {
                "name": "ojt_minio_pyeqx_%s%s",
                "suffix": "",
                "execution": {
                    "inputPath": "./app.ipynb",
                    "outputPath": "output_%s%s.ipynb",
                },
                "datas": {},
                "storages": {
                    "system": {
                        "type": "s3",
                        "properties": {
                            "endpoint": cls.get_minio_endpoint(),
                            "accessKey": cls.MINIO_ACCESS_KEY,
                            "secretKey": cls.MINIO_SECRET_KEY,
                            "bucketName": f"{cls.MINIO_BUCKET}/{cls.MINIO_PATH}",
                        },
                    },
                },
            },
        }

        return Configuration.from_dict(config_dict)
    """

    @classmethod
    def as_dict(cls) -> dict:
        """Return MinIO configuration only (PyEQX config now comes from config.json)"""
        return {
            "minio": cls.get_minio_config(),
            # "pyeqx": cls.get_pyeqx_config(),  # Deprecated - use config.json
        }
