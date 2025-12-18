"""
Standard result classes for data transfer operations
"""

import logging
from typing import Optional, Any, Dict
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class StandardResult:
    """
    Standard result wrapper for data transfer operations

    Provides consistent interface for success/error handling across all transfer operations.

    Usage:
        # Success case
        result = StandardResult.success(
            message="Transfer completed",
            row_count=100,
            source="mongodb",
            target="postgresql"
        )
        # Check status: result.status -> True

        # Error case - logs and raises automatically
        StandardResult.error(
            message="Connection failed",
            error=ConnectionError("Database unavailable")
        )
    """

    # region Special Methods
    def __init__(
        self,
        success: bool,
        message: str,
        error: Optional[Exception] = None,
        **kwargs,
    ):
        """
        Initialize transfer result

        Args:
            success: Whether operation succeeded
            message: Descriptive message
            error: Exception if operation failed
            **kwargs: Additional result data (row_count, source, target, data_df, etc.)
        """
        self.success = success
        self.status = success
        self.message = message
        self.error = error
        self.data = kwargs

    def __repr__(self) -> str:
        """String representation of result"""
        status = "✅ SUCCESS" if self.success else "❌ ERROR"
        data_summary = ", ".join(
            f"{k}={v}" for k, v in self.data.items() if k != "data_df"
        )
        return f"StandardResult({status}: {self.message} | {data_summary})"

    # endregion

    # region Class Methods
    @classmethod
    def success(
        cls,
        message: str,
        row_count: Optional[int] = None,
        source: Optional[str] = None,
        target: Optional[str] = None,
        data_df: Optional[DataFrame] = None,
        **kwargs,
    ) -> "StandardResult":
        """
        Create success result with optional transfer metadata

        Args:
            message: Success message
            row_count: Number of rows transferred
            source: Source database/storage type
            target: Target database/storage type
            data_df: DataFrame with transferred data (optional)
            **kwargs: Additional metadata

        Returns:
            StandardResult instance with status=True
        """
        result_data = {}

        if row_count is not None:
            result_data["row_count"] = row_count
        if source is not None:
            result_data["source"] = source
        if target is not None:
            result_data["target"] = target
        if data_df is not None:
            result_data["data_df"] = data_df

        result_data.update(kwargs)

        return cls(success=True, message=message, error=None, **result_data)

    @classmethod
    def error(cls, message: str, error: Optional[Exception] = None) -> None:
        """
        Log error and raise the original exception

        Args:
            message: Error message to log
            error: Original exception to raise (optional)

        Raises:
            Exception: Re-raises the original exception after logging
        """
        error_msg = f"❌ Error: {str(error)}" if error else f"❌ Error: {message}"
        logger.error(error_msg)
        raise

    # endregion

    # region Static Methods
    @staticmethod
    def show_log_info(
        messages: list[str], level: str = "info", prefix: str = ""
    ) -> None:
        """
        Log multiple messages at specified log level

        Args:
            messages: List of messages to log
            level: Log level - 'debug', 'info', 'warning', 'error' (default: 'info')
            prefix: Optional prefix for each message (e.g., '✅ ', '   ')

        Example:
            StandardResult.show_log_info(
                ["Transfer completed", "100 rows processed"],
                level="info",
                prefix="✅ "
            )
        """
        log_method = getattr(logger, level.lower(), logger.info)

        for message in messages:
            log_method(f"{prefix}{message}")

    # endregion

    # region Public Methods
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert result to dictionary

        Returns:
            dict: Result as dictionary with success, message, error, and data fields
        """
        result = {
            "success": self.success,
            "status": self.status,
            "message": self.message,
            "error": str(self.error) if self.error else None,
        }
        result.update(self.data)
        return result

    # endregion
