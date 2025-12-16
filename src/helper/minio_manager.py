"""MinIO operations using PyEQX framework"""

import glob
import logging
import os
import sys

from minio import Minio

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(parent_dir)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from config.minio_configuration import MinioConfiguration
from config.spark_config import initialize_spark_environment
from helper.standard_result import StandardResult
from pyeqx.core.operation import Operation

logger = logging.getLogger(__name__)

DEFAULT_FILE_PATHS = [
    "/opt/airflow/dags/data/processed",
    "data/airflow/dags/data/processed",
    "data/processed",
]


class MinioManager:
    _instance = None
    _operation = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, table: str = MinioConfiguration.TABLE):
        self.table = table
        self.table_path = f"{MinioConfiguration.get_s3a_uri()}/{self.table}"
        if self._operation is None:
            self._initialize()

    def _initialize(self):
        try:
            config = MinioConfiguration.get_pyeqx_config()
            self._operation = Operation(
                name="MinIO_Operations",
                config=config,
                logger=logger,
            )
            self._operation.initialize_spark(packages=[])
            self.spark = self._operation.get_current_spark_session()

            logger.info("MinIO Operations initialized (Cassandra disabled)")
        except Exception as e:
            StandardResult.error("Failed to initialize MinioManager", error=e)

    @property
    def operation(self):
        return self._operation

    def _create_minio_client(self):
        return Minio(
            endpoint=MinioConfiguration.get_minio_host(),
            access_key=MinioConfiguration.MINIO_ACCESS_KEY,
            secret_key=MinioConfiguration.MINIO_SECRET_KEY,
            secure=False,
        )

    def _create_bucket(self, client, bucket):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)

    def create_bucket_exists(self) -> bool:
        try:
            client = self._create_minio_client()
            bucket = MinioConfiguration.MINIO_BUCKET

            if not client.bucket_exists(bucket):
                return False
            return True
        except Exception as e:
            logger.error(f"Bucket check error: {e}")
            return False

    def _find_file_path(self, filename, local_base_path=None, pattern="part-*.csv"):
        possible_paths = []

        for base_path in DEFAULT_FILE_PATHS:
            possible_paths.append(os.path.join(base_path, filename, pattern))
            possible_paths.append(os.path.join(base_path, filename))

        if local_base_path:
            possible_paths.insert(0, os.path.join(local_base_path, filename))

        for path in possible_paths:
            if "*" in path:
                matches = glob.glob(path)
                if matches:
                    return matches[0]
            elif os.path.exists(path):
                return path

        raise FileNotFoundError(f"File '{filename}' not found in any known location")

    def _build_s3_path(self, path=None, bucket=None, filename=None):
        """
        Build S3 path from various input formats

        Args:
            path: S3 path (can be full URI or relative path)
            bucket: Bucket name (optional, uses default if not provided)
            filename: Filename (optional, uses default path if provided)

        Returns:
            tuple: (s3_path, bucket, object_path)
        """
        if bucket is None:
            bucket = MinioConfiguration.MINIO_BUCKET

        if path:
            if path.startswith("s3a://"):
                s3_path = path
                if "/" in path.replace("s3a://", ""):
                    extracted_bucket = path.replace("s3a://", "").split("/")[0]
                    bucket = extracted_bucket
                    object_path = "/".join(path.replace("s3a://", "").split("/")[1:])
                else:
                    object_path = path.replace(f"s3a://{bucket}/", "")
            else:
                object_path = path
                s3_path = f"s3a://{bucket}/{path}"

        elif filename:
            default_path = MinioConfiguration.MINIO_PATH or "data/csv"
            object_path = f"{default_path}/{filename}"
            s3_path = f"s3a://{bucket}/{object_path}"

        else:
            object_path = MinioConfiguration.MINIO_PATH or "data"
            s3_path = f"s3a://{bucket}/{object_path}"

        return s3_path, bucket, object_path

    def test_connection(self):
        """Test MinIO connection using PySpark S3A and PyEQX reader"""
        try:
            bucket = MinioConfiguration.MINIO_BUCKET

            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            MinioConfiguration.configure_s3a_hadoop(hadoop_conf)

            s3a_uri = f"s3a://{bucket}/"
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark.sparkContext._jvm.java.net.URI(s3a_uri),
                hadoop_conf,
            )
            path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(s3a_uri)

            if not fs.exists(path):
                StandardResult.error(f"Bucket {s3a_uri} does not exist")

            _ = self._operation.get_reader()
            return True

        except Exception as e:
            StandardResult.error("MinIO connection failed", error=e)

    def stop(self):
        self.spark.stop()

    def read_data_from_s3(
        self,
        path: str = None,
        bucket: str = None,
        filename: str = None,
        format: str = "delta",
        schema=None,
        options: dict = None,
    ):
        """
        Read data from S3/MinIO - supports all formats (csv, parquet, delta, json)

        Args:
            path: S3 path (without bucket) e.g. "silver/users" or "data/csv/file.csv"
            bucket: Bucket name (default: from config)
            filename: Filename (if path not specified, use default path + filename)
            format: File format ('csv', 'parquet', 'delta', 'json') (default: 'delta')
            schema: Schema for reading data (optional)
            options: Additional read options (optional)

        Returns:
            DataFrame
        """
        try:
            reader = self._operation.get_reader()
            s3_path, bucket, object_path = self._build_s3_path(path, bucket, filename)
            df = reader.read_from_s3(
                path=s3_path, format=format, schema=schema, options=options
            )

            if df is None:
                StandardResult.error("Failed to read data: DataFrame is None")

            return df

        except Exception as e:
            StandardResult.error("Read from S3 failed", error=e)

    def list_objects(self, prefix: str = "data/csv/") -> dict:
        try:
            client = self._create_minio_client()
            bucket = MinioConfiguration.MINIO_BUCKET
            objects = client.list_objects(
                bucket_name=bucket, prefix=prefix, recursive=True
            )
            object_list = [
                {
                    "name": obj.object_name,
                    "size": obj.size,
                    "last_modified": (
                        obj.last_modified.isoformat() if obj.last_modified else None
                    ),
                }
                for obj in objects
            ]

            return StandardResult.success(
                f"Listed {len(object_list)} objects",
                bucket=bucket,
                prefix=prefix,
                objects=object_list,
                count=len(object_list),
            )

        except Exception as e:
            StandardResult.error("List objects failed", error=e)

    def write_data_to_s3(
        self,
        df=None,
        local_file_path: str = None,
        schema=None,
        s3_path: str = None,
        output_filename: str = None,
        format: str = "delta",
        mode: str = "overwrite",
        options: dict = None,
    ):
        """
        Upload DataFrame or local file to S3 in various formats

        Args:
            df: Spark DataFrame to write (if provided, local_file_path is ignored)
            local_file_path: Local CSV file path to read and upload
            s3_path: S3 path (without bucket)
            output_filename: Output directory/filename
            format: Output format ('delta', 'parquet', 'json', 'csv') (default: 'delta')
            mode: Write mode (overwrite/append)
            options: Additional write options

        Returns:
            dict: Upload result
        """
        try:
            if df is None and local_file_path:
                reader = self._operation.get_reader()

                # Detect source file format from extension
                source_format = "csv"  # default
                if local_file_path.endswith(".parquet"):
                    source_format = "parquet"
                elif local_file_path.endswith(".json"):
                    source_format = "json"

                # Set read options based on source format
                if options is None and source_format == "csv":
                    read_options = {"header": "true", "inferSchema": "true"}
                else:
                    read_options = options

                df = reader.read_from(
                    format=source_format,
                    path=local_file_path,
                    schema=schema,
                    options=read_options,
                )

            if df is None:
                StandardResult.error("Either df or local_file_path is required")

            bucket = MinioConfiguration.MINIO_BUCKET
            writer = self._operation.get_writer()

            # Build S3 path
            if output_filename:
                full_path = f"{bucket}/{s3_path}/{output_filename}"
            else:
                full_path = f"{bucket}/{s3_path}"

            s3a_path = f"s3a://{full_path}"

            if format.lower() == "delta":
                delta_options = options or {
                    "mergeSchema": "true",
                    "overwriteSchema": "true",
                }
                writer.write_to_s3(
                    data=df, mode=mode, path=full_path, options=delta_options
                )
            else:
                writer.write_to(
                    data=df,
                    mode=mode,
                    path=s3a_path,
                    format=format,
                    options=options or {},
                )

        except Exception as e:
            StandardResult.error("Upload to S3 failed", error=e)
