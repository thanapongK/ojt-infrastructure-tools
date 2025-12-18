"""
Unified MongoDB + PyEQX Configuration
Combines MongoDB and PyEQX configuration in a single class
"""

from pyeqx.core.configuration import Configuration
from config import MinioConfiguration


class MongoConfiguration:
    # region MongoDB Constants
    MONGO_HOST = "host.docker.internal"
    MONGO_PORT = 27017
    DATABASE = "ojt_demo"
    TEST_DATABASE = "ojt_test"  # Database for testing
    COLLECTION = "db_connect"
    USER_COLLECTION = "user"

    # Cross-database transfer paths
    TRANSFER_TARGET_PATH = "ojt/mongodb"
    TRANSFER_OUTPUT_FILENAME = "mongodb_json"

    # Database parameter for method calls
    mongo_param = "mongodb"
    DEFAULT_MODE = "overwrite"
    # endregion MongoDB Constants

    # region MongoDB Helper Methods
    @classmethod
    def set_database(cls, db_name: str):
        """Set the active database name

        Args:
            db_name: Database name to use (e.g., 'ojt_test' for testing)
        """
        cls.DATABASE = db_name

    @classmethod
    def use_test_database(cls):
        """Switch to test database"""
        cls.set_database(cls.TEST_DATABASE)

    @classmethod
    def use_production_database(cls):
        """Switch to production database (ojt_demo)"""
        cls.set_database("ojt_demo")

    @classmethod
    def get_mongo_uri(cls) -> str:
        """Return base MongoDB URI (without database)"""
        return f"mongodb://{cls.MONGO_HOST}:{cls.MONGO_PORT}"

    @classmethod
    def get_mongo_uri_with_db(cls) -> str:
        """Return full MongoDB URI including database"""
        return f"{cls.get_mongo_uri()}/{cls.DATABASE}"

    # endregion MongoDB Helper Methods

    # region MongoDB Configuration Methods
    @classmethod
    def get_mongo_config(cls) -> dict:
        """Return MongoDB config as dictionary"""
        return {
            "mongo_host": cls.MONGO_HOST,
            "mongo_port": cls.MONGO_PORT,
            "mongo_uri": cls.get_mongo_uri(),
            "database": cls.DATABASE,
            "collection": cls.COLLECTION,
        }

    # NOTE: get_pyeqx_config() is deprecated - use config.json instead
    # This method created hardcoded local Spark configuration
    # All configuration should now come from config.json
    """
    @classmethod
    def get_pyeqx_config(cls) -> Configuration:
        """Create PyEQX configuration based on Mongo settings"""

        config_dict = {
            "engine": {
                "storage": "system",
                "tmpPath": "/tmp/ojt_mongo/",
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
                "name": "ojt_mongo_pyeqx_%s%s",
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
                            "endpoint": MinioConfiguration.get_minio_endpoint(),
                            "accessKey": MinioConfiguration.MINIO_ACCESS_KEY,
                            "secretKey": MinioConfiguration.MINIO_SECRET_KEY,
                            "bucketName": f"{MinioConfiguration.MINIO_BUCKET}/{MinioConfiguration.MINIO_PATH}",
                        },
                    },
                    "mongodb": {
                        "type": "mongodb",
                        "properties": {
                            "uri": cls.get_mongo_uri(),
                            "db": cls.DATABASE,
                        },
                    },
                },
            },
        }

        return Configuration.from_dict(config_dict)
    """

    @classmethod
    def as_dict(cls) -> dict:
        """Return MongoDB configuration only (PyEQX config now comes from config.json)"""
        return {
            "mongo": cls.get_mongo_config(),
            # "pyeqx": cls.get_pyeqx_config(),  # Deprecated - use config.json
        }

    # endregion MongoDB Configuration Methods
