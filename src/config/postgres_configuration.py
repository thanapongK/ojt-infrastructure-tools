"""
Unified PostgreSQL + PyEQX Configuration
รวม config ของ PostgreSQL และ PyEQX ไว้ใน class เดียว
"""

import os
from pyeqx.core.configuration import Configuration


class PostgresConfiguration:
    # PostgreSQL Constants
    IS_DOCKER = os.path.exists("/.dockerenv")
    POSTGRES_HOST = "localhost" if not IS_DOCKER else "host.docker.internal"
    POSTGRES_PORT = 5432
    POSTGRES_USER = "postgres"
    POSTGRES_PASSWORD = "postgres"
    POSTGRES_DB = "ojt_demo"
    TEST_POSTGRES_DB = "ojt_test"  # Database for testing
    TABLE = "db_connect"

    # Database parameter for method calls
    postgres_param = "postgresql"
    POSTGRES_JDBC_URL = (
        f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    @classmethod
    def set_database(cls, db_name: str):
        """Set the active database name

        Args:
            db_name: Database name to use (e.g., 'ojt_test' for testing)
        """
        cls.POSTGRES_DB = db_name
        cls.POSTGRES_JDBC_URL = (
            f"jdbc:postgresql://{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{db_name}"
        )

    @classmethod
    def use_test_database(cls):
        """Switch to test database"""
        cls.set_database(cls.TEST_POSTGRES_DB)

    @classmethod
    def use_production_database(cls):
        """Switch to production database (ojt_demo)"""
        cls.set_database("ojt_demo")

    @classmethod
    def get_postgres_config(cls) -> dict:
        """Return PostgreSQL config as dictionary"""
        return {
            "postgres_host": cls.POSTGRES_HOST,
            "postgres_port": cls.POSTGRES_PORT,
            "postgres_user": cls.POSTGRES_USER,
            "postgres_password": cls.POSTGRES_PASSWORD,
            "jdbc_url": cls.POSTGRES_JDBC_URL,
            "database": cls.POSTGRES_DB,
            "table": cls.TABLE,
        }

    @classmethod
    def get_pyeqx_config(cls) -> Configuration:
        """Create PyEQX configuration based on PostgreSQL settings"""

        config_dict = {
            "engine": {
                "storage": "system",
                "tmpPath": "/tmp/ojt_postgres/",
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
                },
            },
            "app": {
                "name": "ojt_postgres_pyeqx_%s%s",
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
                            "endpoint": "http://localhost:9000",
                            "accessKey": "user",
                            "secretKey": "password123",
                            "bucketName": "datadd/data",
                        },
                    },
                    "postgresql": {
                        "type": "postgresql",
                        "properties": {
                            "url": cls.POSTGRES_JDBC_URL,
                            "user": cls.POSTGRES_USER,
                            "password": cls.POSTGRES_PASSWORD,
                            "db": cls.POSTGRES_DB,
                        },
                    },
                },
            },
        }
        return Configuration.from_dict(config_dict)

    @classmethod
    def as_dict(cls) -> dict:
        """Return all configuration in unified dictionary"""
        return {
            "postgres": cls.get_postgres_config(),
            "pyeqx": cls.get_pyeqx_config(),
        }
