"""
Spark Configuration - Centralized Environment Setup
Configure Spark environment variables and packages in one place
Shared across all managers (MongoDB, PostgreSQL, MinIO)
"""

import os
import sys


def initialize_spark_environment():
    """
    Initialize Spark environment variables with all required packages

    Packages included:
    - PostgreSQL JDBC driver
    - MongoDB Spark connector
    - Hadoop AWS (for S3/MinIO)
    - AWS Java SDK
    - Delta Lake

    Note: Cassandra connector removed due to Scala version conflicts
    """

    # Clear Kubernetes-related environment variables
    for k in list(os.environ.keys()):
        if "KUBERNETES" in k.upper():
            os.environ.pop(k, None)

    # Set Spark environment variables
    os.environ.update(
        {
            "KUBERNETES_SERVICE_HOST": "",
            "KUBERNETES_SERVICE_PORT": "",
            "SPARK_MASTER": "local[*]",
            "SPARK_LOCAL_IP": "127.0.0.1",
            "PYSPARK_PYTHON": sys.executable,
            "PYSPARK_DRIVER_PYTHON": sys.executable,
            "PYSPARK_SUBMIT_ARGS": (
                "--packages "
                "org.postgresql:postgresql:42.5.4,"
                "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.367,"
                "io.delta:delta-spark_2.12:3.3.2 "
                "pyspark-shell"
            ),
        }
    )


# Check if already initialized to prevent duplicate initialization
if "PYSPARK_SUBMIT_ARGS" not in os.environ:
    initialize_spark_environment()
