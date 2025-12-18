"""MongoDB operations using PyEQX framework"""

import logging
import os
import sys

from pymongo import MongoClient
from pyspark.sql import functions as F

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(parent_dir)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from config.mongo_configuration import MongoConfiguration
from config.spark_config import initialize_spark_environment
from helper.standard_result import StandardResult
from pyeqx.core.operation import Operation

logger = logging.getLogger(__name__)


class MongoDBManeger:
    _instance = None
    _operation = None

    # region Special Methods
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, collection: str = MongoConfiguration.COLLECTION):
        self.collection = collection
        if self._operation is None:
            self._initialize()

    # endregion

    # region Properties
    @property
    def operation(self):
        return self._operation

    # endregion

    # region Private Methods
    def _initialize(self):
        try:
            initialize_spark_environment()
            config = MongoConfiguration.get_pyeqx_config()
            self._operation = Operation(
                name="MongoDB_Operations",
                config=config,
                logger=logging.getLogger(__name__),
            )

            self._operation.initialize_spark(packages=[])
            self.spark = self._operation.get_current_spark_session()
            logging.getLogger(__name__).info("✓ MongoDB Operations initialized")

        except Exception as e:
            StandardResult.error("Failed to initialize MongoDBManager", error=e)

    # endregion

    # region Database Operations - Validate
    def ensure_connection(self):
        try:
            df = self.read_data()
            _ = df.count()
            return True
        except Exception as e:
            StandardResult.error("MongoDB connection failed", error=e)

    # endregion

    # region Database Operations - Read
    def read_data(self, collection: str = None):
        """
        Read data from MongoDB collection

        Args:
            collection: Collection name (if None, uses default self.collection)

        Returns:
            PySpark DataFrame
        """
        try:
            reader = self._operation.get_reader()
            storage_props = self._operation.get_storage_properties("mongodb")

            # Use provided collection or default to self.collection
            collection_name = collection if collection is not None else self.collection

            options = {
                "spark.mongodb.connection.uri": storage_props.uri,
                "spark.mongodb.database": storage_props.db,
                "spark.mongodb.collection": collection_name,
            }

            return reader.read_from(format="mongodb", schema=None, options=options)

        except Exception as e:
            StandardResult.error("Failed to read MongoDB", error=e)

    def read_latest_version(
        self, db_name: str | None = None, collection: str | None = None
    ):
        try:
            collection_name = collection if collection is not None else self.collection
            version_data = self.read_data(collection=collection_name)

            # Check if DataFrame is empty first (avoids schema resolution error)
            if version_data.count() == 0:
                return None

            version_data = version_data.filter(F.col("db") == db_name)
            if version_data.count() == 0:
                return None

            latest = version_data.orderBy(F.col("test_version").desc()).first()
            return latest["test_version"]
        except Exception as e:
            StandardResult.error("Failed to read latest version", error=e)

    # endregion

    # region Database Operations - Write
    def create(
        self,
        db_name: str | None = None,
        version: float = 1.0,
        collection: str | None = None,
    ):
        try:
            collection_name = collection if collection is not None else self.collection

            spark = self._operation.get_current_spark_session()
            new_record_df = spark.range(1).select(
                F.lit(db_name).alias("db"),
                F.lit(float(version)).alias("test_version"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            )

            writer = self._operation.get_writer()
            storage_props = self._operation.get_storage_properties("mongodb")
            writer.write_to_mongodb(
                data=new_record_df,
                mode="append",
                uri=storage_props.uri,
                db=storage_props.db,
                table=collection_name,
                options={},
            )

        except Exception as e:
            StandardResult.error("Failed to create record", error=e)

    def insert_data_to(self, df, collection: str | None = None, mode: str = "append"):
        """
        Insert data to MongoDB collection

        Args:
            df: PySpark DataFrame to insert
            collection: Collection name (if None, uses default self.collection)
            mode: Write mode ('append' or 'overwrite')

        Returns:
            dict: Result with success status and details
        """
        try:
            collection_name = collection if collection is not None else self.collection
            writer = self._operation.get_writer()
            storage_props = self._operation.get_storage_properties("mongodb")
            writer.write_to_mongodb(
                data=df,
                mode=mode,  # append or overwrite
                uri=storage_props.uri,
                db=storage_props.db,
                table=collection_name,
                options={},
            )

        except Exception as e:
            StandardResult.error("Failed to insert data", error=e)

    def update_test_version(
        self, props, db_name: str | None, collection_name: str | None
    ):
        client = MongoClient(props.uri)
        db_conn = client[props.db]
        collection = db_conn[collection_name]
        latest_doc = collection.find_one({"db": db_name}, sort=[("updated_at", -1)])

        if latest_doc:
            collection.delete_many({"db": db_name, "_id": {"$ne": latest_doc["_id"]}})

        client.close()

    def update_test_version(
        self,
        db_name: str | None = None,
        new_version: float | None = None,
        collection: str | None = None,
    ):
        try:
            collection_name = collection if collection is not None else self.collection
            version_data = self.read_data(collection=collection_name)

            if version_data.count() == 0:
                version_to_create = (
                    float(new_version) if new_version is not None else 1.0
                )
                self.create(
                    db_name=db_name,
                    version=version_to_create,
                    collection=collection_name,
                )
                return

            version_data = version_data.filter(F.col("db") == db_name)

            if version_data.count() == 0:
                version_to_create = (
                    float(new_version) if new_version is not None else 1.0
                )
                self.create(
                    db_name=db_name,
                    version=version_to_create,
                    collection=collection_name,
                )
                return

            latest_row = version_data.orderBy(F.col("test_version").desc()).first()
            current_version = latest_row["test_version"]
            existing_id = latest_row["_id"]
            created_at = latest_row["created_at"]
            final_version = round(float(current_version) + 0.1, 1)

            if new_version is not None:
                final_version = float(new_version)

            spark = self._operation.get_current_spark_session()
            new_record_df = spark.range(1).select(
                F.lit(existing_id).alias("_id"),
                F.lit(db_name).alias("db"),
                F.lit(final_version).alias("test_version"),
                F.lit(created_at).alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            )

            writer = self._operation.get_writer()
            props = self._operation.get_storage_properties("mongodb")
            writer.write_to_mongodb(
                data=new_record_df,
                mode="append",
                uri=props.uri,
                db=props.db,
                table=collection_name,
                options={},
            )

            self.delete_old_records(props, db_name, collection_name)

        except Exception as e:
            StandardResult.error("Failed to update test_version", error=e)

    def insert_data_to(self, df, collection: str | None = None, mode: str = "append"):
        """
        Insert data to MongoDB collection

        Args:
            df: PySpark DataFrame to insert
            collection: Collection name (if None, uses default self.collection)
            mode: Write mode ('append' or 'overwrite')

        Returns:
            dict: Result with success status and details
        """
        try:
            collection_name = collection if collection is not None else self.collection
            writer = self._operation.get_writer()
            storage_props = self._operation.get_storage_properties("mongodb")
            writer.write_to_mongodb(
                data=df,
                mode=mode,  # append or overwrite
                uri=storage_props.uri,
                db=storage_props.db,
                table=collection_name,
                options={},
            )

        except Exception as e:
            StandardResult.error("Failed to insert data", error=e)

    # endregion

    # region Database Operations - Delete
    def delete_old_records(
        self, props, db_name: str | None, collection_name: str | None
    ):
        client = MongoClient(props.uri)
        db_conn = client[props.db]
        collection = db_conn[collection_name]
        latest_doc = collection.find_one({"db": db_name}, sort=[("updated_at", -1)])

        if latest_doc:
            collection.delete_many({"db": db_name, "_id": {"$ne": latest_doc["_id"]}})

        client.close()

    # endregion

    # region Database Operations - Cleanup
    def stop(self):
        self.spark.stop()

    # endregion
