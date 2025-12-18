"""PostgreSQL operations using PyEQX framework"""

import logging
import os
import sys

import psycopg2
from pyspark.sql import functions as F

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(parent_dir)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from config import PostgresConfiguration
from config.spark_config import initialize_spark_environment
from helper import StandardResult
from pyeqx.core.operation import Operation

logger = logging.getLogger(__name__)


class PostgresManager:
    _instance = None
    _operation = None

    # region Special Methods
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, table: str = PostgresConfiguration.TABLE):
        self.table = table
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
            config = PostgresConfiguration.get_pyeqx_config()
            self._operation = Operation(
                name="PostgreSQL_Operations",
                config=config,
                logger=logging.getLogger(__name__),
            )
            self._operation.initialize_spark(packages=[])
            self.spark = self._operation.get_current_spark_session()

            logging.getLogger(__name__).info("✓ PostgreSQL Operations initialized")

        except Exception as e:
            StandardResult.error("Failed to initialize PostgresManager", error=e)

    # endregion

    # region Database Operations - Validate
    def ensure_connection(self) -> bool:
        try:
            test_data = self.read_data()
            _ = test_data.count()
            return True
        except Exception as e:
            StandardResult.error("PostgreSQL connection failed", error=e)

    # endregion

    # region Database Operations - Read
    def read_data(self, table: str = None):
        """
        Read data from PostgreSQL table

        Args:
            table: Table name (if None, uses default self.table)

        Returns:
            PySpark DataFrame
        """
        try:
            reader = self._operation.get_reader()
            storage_props = self._operation.get_storage_properties("postgresql")
            table_name = table if table is not None else self.table

            return reader.read_from_sql(
                table=table_name,
                storage_props=storage_props,
                schema=None,
                options={
                    "driver": "org.postgresql.Driver",
                    "user": PostgresConfiguration.POSTGRES_USER,
                    "password": PostgresConfiguration.POSTGRES_PASSWORD,
                },
            )

        except Exception as e:
            StandardResult.error("Failed to read PostgreSQL", error=e)

    def read_latest_version(
        self, db_name: str | None = None, table: str | None = None
    ) -> float | None:
        try:
            table_name = table if table is not None else self.table
            version_data = self.read_data(table=table_name)

            if version_data.count() == 0:
                return None

            filtered_data = version_data.filter(F.col("db") == db_name)
            if filtered_data.count() == 0:
                return None

            latest_record = filtered_data.orderBy(F.col("test_version").desc()).first()
            return latest_record["test_version"]
        except Exception as e:
            StandardResult.error("Failed to read latest version", error=e)

    # endregion

    # region Database Operations - Write
    def create(
        self,
        db_name: str | None = None,
        version: float = 1.0,
        table: str | None = None,
    ) -> None:
        try:
            table_name = table if table is not None else self.table
            spark = self._operation.get_current_spark_session()

            new_version_record = spark.range(1).select(
                F.lit(db_name).alias("db"),
                F.lit(float(version)).alias("test_version"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            )

            writer = self._operation.get_writer()
            writer.write_to_postgresql(
                data=new_version_record,
                mode="append",
                url=PostgresConfiguration.POSTGRES_JDBC_URL,
                table=table_name,
                options={
                    "driver": "org.postgresql.Driver",
                    "user": PostgresConfiguration.POSTGRES_USER,
                    "password": PostgresConfiguration.POSTGRES_PASSWORD,
                },
            )

        except Exception as e:
            StandardResult.error("Failed to create record", error=e)

    def insert_data_to(
        self, data, table: str | None = None, mode: str = "append"
    ) -> None:
        """
        Insert data to PostgreSQL table

        Args:
            data: PySpark DataFrame to insert
            table: Table name (if None, uses default self.table)
            mode: Write mode ('append' or 'overwrite')

        Note:
            Table names are automatically converted to lowercase and quoted to:
            1. Ensure case-insensitive behavior (quoted lowercase = standard PostgreSQL)
            2. Handle reserved keywords safely ('user', 'order', 'group', etc.)
            3. Maintain consistency across all operations

            For tables with auto-increment ID (like 'user'), the 'id' column is removed
            from the DataFrame before insert, allowing PostgreSQL to generate it automatically.
        """
        try:
            table_name = table.lower() if table else self.table.lower()
            quoted_table = f'"{table_name}"'

            # Remove auto-increment ID column if present
            insert_data = data.drop("id") if "id" in data.columns else data
            expected_count = insert_data.count()

            # Write to PostgreSQL
            writer = self._operation.get_writer()
            writer.write_to_postgresql(
                data=insert_data,
                mode=mode,
                url=PostgresConfiguration.POSTGRES_JDBC_URL,
                table=quoted_table,
                options={
                    "driver": "org.postgresql.Driver",
                    "user": PostgresConfiguration.POSTGRES_USER,
                    "password": PostgresConfiguration.POSTGRES_PASSWORD,
                },
            )

            # Verify insertion
            actual_count = self.read_data(table=quoted_table).count()
            if actual_count == 0 and expected_count > 0:
                StandardResult.error(
                    f"Insert verification failed: Expected {expected_count} rows but found 0 in {quoted_table}"
                )

        except Exception as e:
            StandardResult.error("Failed to insert data", error=e)

    def update_test_version(
        self,
        db_name: str | None = None,
        new_version: float | None = None,
        table: str | None = None,
    ) -> None:
        try:
            table_name = table if table is not None else self.table
            version_data = self.read_data(table=table_name)

            # If table is empty, create initial version
            if version_data.count() == 0:
                initial_version = float(new_version) if new_version else 1.0
                self.create(db_name=db_name, version=initial_version, table=table_name)
                return

            # Filter by database name
            filtered_data = version_data.filter(F.col("db") == db_name)
            if filtered_data.count() == 0:
                initial_version = float(new_version) if new_version else 1.0
                self.create(db_name=db_name, version=initial_version, table=table_name)
                return

            # Get latest version record
            latest_record = filtered_data.orderBy(F.col("test_version").desc()).first()
            current_version = latest_record["test_version"]
            created_at_value = latest_record["created_at"]

            # Calculate new version
            final_version = (
                float(new_version)
                if new_version
                else round(float(current_version) + 0.1, 1)
            )

            # Create updated version record
            spark = self._operation.get_current_spark_session()
            updated_version_record = spark.range(1).select(
                F.lit(db_name).alias("db"),
                F.lit(final_version).alias("test_version"),
                F.lit(created_at_value).alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            )

            # Write to database
            writer = self._operation.get_writer()
            writer.write_to_postgresql(
                data=updated_version_record,
                mode="append",
                url=PostgresConfiguration.POSTGRES_JDBC_URL,
                table=table_name,
                options={
                    "driver": "org.postgresql.Driver",
                    "user": PostgresConfiguration.POSTGRES_USER,
                    "password": PostgresConfiguration.POSTGRES_PASSWORD,
                },
            )

            # Clean up old records
            self.delete_old_records(db_name, table_name)

        except Exception as e:
            StandardResult.error("Failed to update test_version", error=e)

    # endregion

    # region Database Operations - Delete
    def delete_old_records(self, db_name: str | None, table_name: str | None) -> None:
        conn = psycopg2.connect(
            host=PostgresConfiguration.POSTGRES_HOST,
            port=PostgresConfiguration.POSTGRES_PORT,
            database=PostgresConfiguration.POSTGRES_DB,
            user=PostgresConfiguration.POSTGRES_USER,
            password=PostgresConfiguration.POSTGRES_PASSWORD,
        )
        cursor = conn.cursor()
        cursor.execute(
            f"""DELETE FROM {table_name}
                WHERE db = %s
                AND id NOT IN (
                    SELECT id FROM {table_name}
                    WHERE db = %s
                    ORDER BY updated_at DESC
                    LIMIT 1
                )""",
            (db_name, db_name),
        )
        conn.commit()
        cursor.close()
        conn.close()

    # endregion

    # region Database Operations - Cleanup
    def stop(self):
        self.spark.stop()

    # endregion
