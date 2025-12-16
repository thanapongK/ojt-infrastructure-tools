import argparse
import logging
import os
from pprint import pformat

# log_format = (
#     "%(asctime)s %(levelname)s **%(filename)s**:%(lineno)d %(name)s: %(message)s"
# )
log_format = "%(asctime)s %(levelname)s: %(message)s"

logging.basicConfig(level=logging.INFO, format=log_format)

working_dir = os.getcwd()


def debug(logger: logging.Logger, arguments: argparse.Namespace):
    logger.debug("debugging: environment variables")
    logger.debug(pformat(os.environ))
    # for key, value in sorted(os.environ.items()):
    #     logger.debug(f"{key}={value}")

    logger.debug(f"debugging: arguments: {arguments}")
    logger.debug(f"debugging: working directory: {working_dir}")


def main():
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(description="Run with parameters.")

    parser.add_argument(
        "--dag-id",
        type=str,
        help="DAG ID",
        required=True,
    )

    parser.add_argument(
        "--task", type=str, help="Task", required=False, default="spark"
    )

    parser.add_argument(
        "--operation",
        type=str,
        help="Operation type",
        required=False,
        default="full_workflow",
    )

    args = parser.parse_args()

    debug(logger=logger, arguments=args)

    if args.task:
        # ========== Database Operations Tasks ==========
        if args.task == "minio_operations":
            from app.task_minio_operations import run_minio_process

            logger.info(f"Running task: {args.task}")
            run_minio_process(
                logger=logger,
                working_dir=working_dir,
                config_path="/app/config/config.json",
                arguments={
                    "name": "infrastructure-testing-tools-minio",
                    "dag_id": args.dag_id,
                    "operation": args.operation,
                },
            )

        elif args.task == "postgres_operations":
            from app.task_postgres_operations import run_postgres_process

            logger.info(f"Running task: {args.task}")
            run_postgres_process(
                logger=logger,
                working_dir=working_dir,
                config_path="/app/config/config.json",
                arguments={
                    "name": "infrastructure-testing-tools-postgres",
                    "dag_id": args.dag_id,
                    "operation": args.operation,
                },
            )

        elif args.task == "mongodb_operations":
            from app.task_mongodb_operations import run_mongodb_process

            logger.info(f"Running task: {args.task}")
            run_mongodb_process(
                logger=logger,
                working_dir=working_dir,
                config_path="/app/config/config.json",
                arguments={
                    "name": "infrastructure-testing-tools-mongodb",
                    "dag_id": args.dag_id,
                    "operation": args.operation,
                },
            )

        elif args.task == "cross_database_operations":
            from app.task_cross_database_operations import run_cross_database_process

            logger.info(f"Running task: {args.task}")
            run_cross_database_process(
                logger=logger,
                working_dir=working_dir,
                config_path="/app/config/config.json",
                arguments={
                    "name": "infrastructure-testing-tools-cross-database",
                    "dag_id": args.dag_id,
                    "operation": args.operation,
                },
            )

        else:
            logger.error(f"Unknown task: {args.task}")
    else:
        logger.error("No task specified")


if __name__ == "__main__":
    main()
