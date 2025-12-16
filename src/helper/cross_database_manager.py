import logging
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(parent_dir)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from helper.mongodb_manager import MongoDBManeger
from helper.postgres_manager import PostgresManager
from helper.minio_manager import MinioManager
from helper.user_validator import validate_all
from helper.standard_result import StandardResult

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CrossDatabaseManager:
    """
    Manager for cross-database operations
    Supports: data transfer
    """

    def __init__(self):
        self.mongo_manager = None
        self.postgres_manager = None
        self.minio_manager = None
        self.supported_databases = ["mongodb", "postgresql", "minio"]

    def _get_manager(self, db_type: str):
        """Get or create manager based on database type"""
        if db_type not in ["mongodb", "postgresql", "minio", "s3"]:
            raise ValueError(f"Unsupported database type: {db_type}")

        if db_type == "mongodb":
            if self.mongo_manager is None:
                self.mongo_manager = MongoDBManeger()
            return self.mongo_manager
        elif db_type == "postgresql":
            if self.postgres_manager is None:
                self.postgres_manager = PostgresManager()
            return self.postgres_manager
        else:  # minio or s3
            if self.minio_manager is None:
                self.minio_manager = MinioManager()
            return self.minio_manager

    def test_connections(self, db_type: list = None):
        """
        Test connections to multiple databases and raise error if any fail

        Args:
            db_type: List of database types to test ['mongodb', 'postgresql', 's3']
                     If None, tests all supported databases

        Returns:
            bool: True if all connections succeed

        Raises:
            RuntimeError: If any database connection fails
        """
        if db_type is None:
            db_type = ["mongodb", "postgresql", "s3"]

        connection_errors = []

        for db in db_type:
            try:
                self._get_manager(db).test_connection()
            except Exception as e:
                logger.error(f"❌ {db}: {e}")
                connection_errors.append(f"❌ {db}: {str(e)}")

        if connection_errors:
            error_summary = "\n".join(connection_errors)
            logger.error(f"\n🚨 Connection Failures:\n{error_summary}")
            raise RuntimeError(f"Connection test failed:\n{error_summary}")

        return True

    def read_from_database(self, db_type: str, container: str = None):
        """
        Read data from database using unified container parameter.

        Args:
            db_type: Database type ('mongodb' or 'postgresql')
            container: Collection (MongoDB) or Table (PostgreSQL) name

        Returns:
            PySpark DataFrame
        """
        if db_type not in ["mongodb", "postgresql"]:
            raise ValueError(
                f"Unsupported database type: {db_type}. Use 'mongodb' or 'postgresql' only."
            )

        manager = self._get_manager(db_type)

        if db_type == "mongodb":
            return manager.read_data(collection=container)
        else:  # postgresql
            return manager.read_data(table=container)

    def write_to_database(
        self, df, db_type: str, name: str = None, mode: str = "append"
    ):
        """
        Unified interface to write data to database (MongoDB or PostgreSQL)

        Args:
            df: PySpark DataFrame to write
            db_type: Database type ('mongodb', 'postgresql')
            name: Collection name (for MongoDB) or Table name (for PostgreSQL)
            mode: Write mode ('append', 'overwrite') (default: 'append')

        Raises:
            RuntimeError: If write operation fails
        """
        if db_type not in ["mongodb", "postgresql"]:
            raise ValueError(
                f"Unsupported database type: {db_type}. Use 'mongodb' or 'postgresql' only."
            )

        manager = self._get_manager(db_type)

        try:
            if db_type == "mongodb":
                manager.insert_data_to(df=df, collection=name, mode=mode)
            else:
                manager.insert_data_to(df=df, table=name, mode=mode)
        except Exception as e:
            logger.error(f"✗ Write failed {db_type}: {e}")
            raise

    def write_to_storage(self, df, **kwargs):
        """
        Write data to Object Storage (S3/MinIO)

        Args:
            df: PySpark DataFrame to write
            **kwargs: Storage-specific parameters

        S3/MinIO kwargs:
            - path: S3 path to write to (required)
            - format: File format ('csv', 'parquet', 'delta', 'json') (default: 'delta')
            - mode: Write mode ('append', 'overwrite') (default: 'overwrite')
            - output_filename: Output filename (optional, e.g., 'mongodb.csv')
            - partition_by: Column(s) to partition by (optional)
            - options: Additional write options (optional)

        Raises:
            RuntimeError: If write operation fails
        """
        manager = self._get_manager("minio")
        path = kwargs.get("path")
        format = kwargs.get("format", "delta")
        mode = kwargs.get("mode", "overwrite")
        output_filename = kwargs.get("output_filename")
        options = kwargs.get("options")

        if not path:
            raise ValueError("S3/MinIO write requires 'path' parameter")

        manager.write_data_to_s3(
            df=df,
            s3_path=path,
            output_filename=output_filename,
            format=format,
            mode=mode,
            options=options,
        )

    def read_from_storage(self, **kwargs):
        """
        Read data from Object Storage (MinIO/S3)

        Args:
            **kwargs: Storage-specific parameters

        MinIO kwargs:
            - path: S3 path to read from (required)
            - format: File format ('csv', 'parquet', 'delta', 'json') (default: 'delta')

        Returns:
            PySpark DataFrame
        """
        manager = self._get_manager("minio")
        path = kwargs.get("path")
        file_format = kwargs.get("format", "delta")

        if not path:
            raise ValueError("MinIO read requires 'path' parameter")

        return manager.read_data_from_s3(path=path, format=file_format)

    def write_version_to_target(
        self,
        source_db: str,
        target_db: str,
        db_name: str = None,
    ):
        """Write version from source to target database"""
        db_name_to_use = db_name if db_name is not None else source_db
        source_version = (
            self._get_manager(source_db).read_latest_version(db_name=db_name_to_use)
            or 1.0
        )
        self._get_manager(target_db).update_test_version(
            db_name=db_name_to_use, new_version=source_version
        )

    def cross_version_to(
        self,
        read: str,
        write: str,
        db_name: str = None,
    ):
        """
        Cross-database version transfer
        Args:
            read: Source database type ('mongodb' or 'postgresql')
            write: Target database type ('mongodb' or 'postgresql')
            db_name: Database name to transfer (if None, uses read database type as db_name)

        Returns:
            dict: Result with success status and transfer details
        """
        try:
            self.test_connections(db_type=[read, write])
            self.write_version_to_target(
                source_db=read, target_db=write, db_name=db_name
            )

        except Exception as e:
            raise RuntimeError(f"Version transfer failed {read}->{write}: {str(e)}")

    def cross_data_to(self, read: str, write: str, **kwargs):
        """
        Cross-database/storage data transfer with flexible parameters

        Args:
            read: Source ('mongodb', 'postgresql', 's3')
            write: Target ('mongodb', 'postgresql', 's3')
            **kwargs: Flexible parameters for read/write operations

        Database kwargs:
            - source_collection: MongoDB collection to read from
            - source_table: PostgreSQL table to read from
            - target_collection: MongoDB collection to write to
            - target_table: PostgreSQL table to write to
            - mode: Write mode ('append', 'overwrite')

        Storage (S3/MinIO) kwargs:
            - source_path: S3 path to read from
            - target_path: S3 path to write to
            - format: File format ('csv', 'parquet', 'delta')
            - output_filename: Output filename for storage write

        Returns:
            dict: {
                "success": True,
                "row_count": int,
                "source": str,
                "target": str
            }
        """
        try:
            database_types = ["mongodb", "postgresql"]
            storage_types = ["s3", "minio"]

            if read not in database_types + storage_types:
                raise ValueError(f"Unsupported source type: {read}")

            if write not in database_types + storage_types:
                raise ValueError(f"Unsupported target type: {write}")

            if read in database_types:
                container = kwargs.get("source_collection") or kwargs.get(
                    "source_table"
                )
                if not container:
                    raise ValueError(
                        f"Missing source container for {read} (use source_collection or source_table)"
                    )

                df = self.read_from_database(db_type=read, container=container)

            elif read in storage_types:
                source_path = kwargs.get("source_path")

                if not source_path:
                    raise ValueError(f"Missing source_path for {read}")

                read_kwargs = {
                    "path": source_path,
                    "format": kwargs.get("format", "parquet"),
                }
                df = self.read_from_storage(**read_kwargs)

            row_count = df.count()
            if row_count == 0:
                raise ValueError(f"No data found in source: {read}")

            source_type = read if read not in storage_types else "s3"
            target_type = write if write not in storage_types else "s3"
            self.test_connections(db_type=[source_type, target_type])

            if write in database_types:
                target_name = kwargs.get("target_collection") or kwargs.get(
                    "target_table"
                )
                if not target_name:
                    raise ValueError(
                        f"Missing target container for {write} (use target_collection or target_table)"
                    )
                mode = kwargs.get("mode", "append")
                self.write_to_database(
                    df=df, db_type=write, name=target_name, mode=mode
                )

            elif write in storage_types:
                target_path = kwargs.get("target_path")
                if not target_path:
                    raise ValueError(f"Missing target_path for {write}")
                write_kwargs = {
                    "path": target_path,
                    "format": kwargs.get("format", "delta"),
                    "mode": kwargs.get("mode", "overwrite"),
                    "output_filename": kwargs.get("output_filename"),
                    "partition_by": kwargs.get("partition_by"),
                    "options": kwargs.get("options"),
                }
                self.write_to_storage(df=df, **write_kwargs)

            return {
                "success": True,
                "row_count": row_count,
                "source": read,
                "target": write,
            }

        except Exception as e:
            raise RuntimeError(f"Transfer failed {read}->{write}: {str(e)}") from e

    def transfer_validated_user_data(
        self, write: str, source_path: str, target_name: str, mode: str = "overwrite"
    ):
        """
        Transfer user data from S3 to database with validation

        Read user data from S3, validate using UserValidator, then write valid records to database.
        Invalid records are filtered out automatically.

        Args:
            write: Target database ('mongodb' or 'postgresql')
            source_path: S3 path to read user data (e.g., 's3a://ojtbucket/silver/raw_valid_data_user')
            target_name: Collection name (MongoDB) or Table name (PostgreSQL)
            mode: Write mode ('append', 'overwrite') (default: 'overwrite')

        Returns:
            dict: Result with success status and validation details
        """
        try:
            self.test_connections(db_type=["s3", write])

            df = self.read_from_storage(path=source_path, format="delta")
            total_records = df.count()

            if total_records == 0:
                raise ValueError(f"No data found in source: {source_path}")

            validated_df = validate_all(df, auto_fix_headers=True)
            valid_records = validated_df.count()

            if valid_records == 0:
                raise ValueError("No valid records after validation")

            self.write_to_database(
                df=validated_df, db_type=write, name=target_name, mode=mode
            )

            invalid_records = total_records - valid_records
            return (total_records, valid_records, invalid_records)

        except Exception as e:
            raise RuntimeError(
                f"Validated transfer failed {source_path}->{write}: {str(e)}"
            )

    def cleanup(self):
        """Cleanup resources and close connections"""
        if self.mongo_manager:
            self.mongo_manager.stop()
        if self.postgres_manager:
            self.postgres_manager.stop()
        if self.minio_manager:
            self.minio_manager.stop()
