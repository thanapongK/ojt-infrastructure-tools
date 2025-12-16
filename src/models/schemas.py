"""
Central schema definitions for all data structures
รวม schema ทั้งหมดไว้ที่เดียว
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    IntegerType,
)


def get_raw_valid_data_user_schema() -> StructType:
    """
    Schema for raw_valid_data_user CSV file

    Columns:
    - first_name: string
    - last_name: string
    - email: string
    - phone: string
    - gender: string (M/F)

    Returns:
        StructType: PySpark schema for raw valid user data
    """
    return StructType(
        [
            StructField("first_name", StringType(), nullable=True),
            StructField("last_name", StringType(), nullable=True),
            StructField("email", StringType(), nullable=True),
            StructField("phone", StringType(), nullable=True),
            StructField("gender", StringType(), nullable=True),
        ]
    )


def get_db_connect_schema() -> StructType:
    """
    Schema for db_connect collection (MongoDB/PostgreSQL)

    Columns:
    - db: string (database name)
    - test_version: double (version number)
    - created_at: timestamp
    - updated_at: timestamp

    Returns:
        StructType: PySpark schema for db_connect table
    """
    return StructType(
        [
            StructField("db", StringType(), nullable=False),
            StructField("test_version", DoubleType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=False),
            StructField("updated_at", TimestampType(), nullable=False),
        ]
    )


def get_user_schema() -> StructType:
    """
    Schema for user table (MongoDB collection / PostgreSQL table)
    Based on raw_valid_data_user with auto-increment ID

    Columns:
    - id: integer (auto-increment primary key for PostgreSQL)
    - first_name: string
    - last_name: string
    - email: string
    - phone: string
    - gender: string (M/F)

    Returns:
        StructType: PySpark schema for user table

    Note:
        MongoDB uses _id as primary key automatically.
        PostgreSQL needs explicit ID column with SERIAL/auto-increment.
    """
    return StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("first_name", StringType(), nullable=True),
            StructField("last_name", StringType(), nullable=True),
            StructField("email", StringType(), nullable=True),
            StructField("phone", StringType(), nullable=True),
            StructField("gender", StringType(), nullable=True),
        ]
    )


# Schema registry - สำหรับเรียกใช้แบบ dynamic
SCHEMAS = {
    "raw_valid_data_user": get_raw_valid_data_user_schema,
    "db_connect": get_db_connect_schema,
    "user": get_user_schema,
}


def get_schema(schema_name: str) -> StructType:
    """
    Get schema by name from registry

    Args:
        schema_name: Name of the schema ('raw_valid_data_user', 'db_connect', 'user')

    Returns:
        StructType: PySpark schema

    Raises:
        ValueError: If schema name not found

    Example:
        >>> schema = get_schema("raw_valid_data_user")
        >>> df = spark.read.csv("file.csv", schema=schema)
    """
    if schema_name not in SCHEMAS:
        available = ", ".join(SCHEMAS.keys())
        raise ValueError(
            f"Schema '{schema_name}' not found. Available schemas: {available}"
        )

    return SCHEMAS[schema_name]()


# ============================================================================
# SQL COMMANDS for Table Creation
# ============================================================================


def get_create_user_table_sql() -> str:
    """
    Generate SQL command to create 'user' table in PostgreSQL

    Table: user
    - id: SERIAL PRIMARY KEY (auto-increment)
    - first_name: VARCHAR(255)
    - last_name: VARCHAR(255)
    - email: VARCHAR(255)
    - phone: VARCHAR(50)
    - gender: VARCHAR(1) (M/F)

    Returns:
        str: SQL CREATE TABLE command

    Example:
        >>> sql = get_create_user_table_sql()
        >>> print(sql)
    """
    return """
-- Create user table in PostgreSQL
-- Auto-increment ID with SERIAL type

DROP TABLE IF EXISTS "user" CASCADE;

CREATE TABLE "user" (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    gender VARCHAR(1) CHECK (gender IN ('M', 'F'))
);

-- Create index on email for faster lookups
CREATE INDEX idx_user_email ON "user"(email);

-- Add comments
COMMENT ON TABLE "user" IS 'User data table based on raw_valid_data_user';
COMMENT ON COLUMN "user".id IS 'Auto-increment primary key';
COMMENT ON COLUMN "user".email IS 'User email address';
COMMENT ON COLUMN "user".gender IS 'User gender: M (Male) or F (Female)';
"""
