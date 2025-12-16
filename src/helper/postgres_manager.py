"""PostgreSQL operations using PyEQX framework"""

import logging
import os
import sys

import psycopg2
from pyspark.sql.functions import col, current_timestamp, lit

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(parent_dir)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from config.postgres_configuration import PostgresConfiguration
from config.spark_config import initialize_spark_environment
from helper.standard_result import StandardResult
from pyeqx.core.operation import Operation

logger = logging.getLogger(__name__)


class PostgresManager:
    _instance = None
    _operation = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, table: str = PostgresConfiguration.TABLE):
        self.table = table
        if self._operation is None:
            self._initialize()

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

    @property
    def operation(self):
        return self._operation

    def test_connection(self):
        try:
            df = self.read_data()
            _ = df.count()
            return True
        except Exception as e:
            StandardResult.error("PostgreSQL connection failed", error=e)

    def stop(self):
        self.spark.stop()

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

    def read_latest_version(self, db_name: str | None = None, table: str | None = None):
        try:
            table_name = table if table is not None else self.table
            df = self.read_data(table=table_name)
            if df.count() == 0:
                return None

            df = df.filter(col("db") == db_name)
            if df.count() == 0:
                return None

            latest = df.orderBy(col("test_version").desc()).first()
            return latest["test_version"]
        except Exception as e:
            StandardResult.error("Failed to read latest version", error=e)

    def create(
        self,
        db_name: Optional[str] = None,
        version: float = 1.0,
        table: Optional[str] = None,
    ):
        try:
            table_name = table if table is not None else self.table

            spark = self._operation.get_current_spark_session()
            new_record_df = spark.range(1).select(
                lit(db_name).alias("db"),
                lit(float(version)).alias("test_version"),
                current_timestamp().alias("created_at"),
                current_timestamp().alias("updated_at"),
            )

            writer = self._operation.get_writer()
            jdbc_url = PostgresConfiguration.POSTGRES_JDBC_URL
            writer.write_to_postgresql(
                data=new_record_df,
                mode="append",
                url=jdbc_url,
                table=table_name,
                options={
                    "driver": "org.postgresql.Driver",
                    "user": PostgresConfiguration.POSTGRES_USER,
                    "password": PostgresConfiguration.POSTGRES_PASSWORD,
                },
            )

        except Exception as e:
            StandardResult.error("Failed to create record", error=e)

    def delete_old_records(self, db_name: str | None, table_name: str | None):
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

    def update_test_version(
        self,
        db_name: str | None = None,
        new_version: float | None = None,
        table: str | None = None,
    ):
        try:
            table_name = table if table is not None else self.table
            df = self.read_data(table=table_name)

            if df.count() == 0:
                version_to_create = (
                    float(new_version) if new_version is not None else 1.0
                )
                self.create(
                    db_name=db_name, version=version_to_create, table=table_name
                )
                return

            df = df.filter(col("db") == db_name)
            if df.count() == 0:
                version_to_create = (
                    float(new_version) if new_version is not None else 1.0
                )
                self.create(
                    db_name=db_name, version=version_to_create, table=table_name
                )
                return

            latest_row = df.orderBy(col("test_version").desc()).first()
            current_version = latest_row["test_version"]
            created_at_value = latest_row["created_at"]

            if new_version is not None:
                final_version = float(new_version)
            else:
                final_version = round(float(current_version) + 0.1, 1)

            spark = self._operation.get_current_spark_session()
            new_record_df = spark.range(1).select(
                lit(db_name).alias("db"),
                lit(final_version).alias("test_version"),
                lit(created_at_value).alias("created_at"),
                current_timestamp().alias("updated_at"),
            )

            writer = self._operation.get_writer()
            jdbc_url = PostgresConfiguration.POSTGRES_JDBC_URL

            writer.write_to_postgresql(
                data=new_record_df,
                mode="append",
                url=jdbc_url,
                table=table_name,
                options={
                    "driver": "org.postgresql.Driver",
                    "user": PostgresConfiguration.POSTGRES_USER,
                    "password": PostgresConfiguration.POSTGRES_PASSWORD,
                },
            )

            self.delete_old_records(db_name, table_name)

        except Exception as e:
            StandardResult.error("Failed to update test_version", error=e)

    def insert_data_to(self, df, table: str | None = None, mode: str = "append"):
        """
        Insert data to PostgreSQL table

        Args:
            df: PySpark DataFrame to insert
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
            table_name = table.lower() if table is not None else self.table.lower()
            quoted_table = f'"{table_name}"'

            if "id" in df.columns:
                df = df.drop("id")

            row_count = df.count()

            writer = self._operation.get_writer()
            jdbc_url = PostgresConfiguration.POSTGRES_JDBC_URL

            writer.write_to_postgresql(
                data=df,
                mode=mode,
                url=jdbc_url,
                table=quoted_table,
                options={
                    "driver": "org.postgresql.Driver",
                    "user": PostgresConfiguration.POSTGRES_USER,
                    "password": PostgresConfiguration.POSTGRES_PASSWORD,
                },
            )

            actual_count = self.read_data(table=quoted_table).count()

            if actual_count == 0 and row_count > 0:
                StandardResult.error(
                    f"Insert verification failed: Expected {row_count} rows but found 0 in {quoted_table}"
                )

        except Exception as e:
            StandardResult.error("Failed to insert data", error=e)
