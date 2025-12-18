"""
User Data Validation Utility
Provides validation functions for user DataFrame operations
"""

import logging
from typing import Tuple, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from helper import StandardResult

logger = logging.getLogger(__name__)


class UserValidator:
    """Validator for user data operations"""

    # Expected column headers
    REQUIRED_HEADERS = ["first_name", "last_name", "email", "phone", "gender"]

    # Validation patterns
    EMAIL_PATTERN = r"^[a-zA-Z0-9._%+-]+@ais\.com$"
    PHONE_PATTERN = (
        r"^(06|08|09)\d{8}$"  # Must start with 06, 08, or 09 followed by 8 digits
    )

    @staticmethod
    def validate_headers(df: DataFrame) -> bool:
        if df is None or df.count() == 0:
            return True

        actual_columns = df.columns
        required_set = set(UserValidator.REQUIRED_HEADERS)
        actual_set = set(actual_columns)

        missing = list(required_set - actual_set)
        extra = list(actual_set - required_set)

        if missing or extra:
            return False

        if actual_columns != UserValidator.REQUIRED_HEADERS:
            return False

        return True

    @staticmethod
    def fix_headers(df: DataFrame) -> DataFrame:
        if df is None or df.count() == 0:
            return df

        try:
            actual_columns = df.columns
            required_set = set(UserValidator.REQUIRED_HEADERS)
            actual_set = set(actual_columns)

            missing = list(required_set - actual_set)
            extra = list(actual_set - required_set)

            df_fixed = df.select(
                [col_name for col_name in actual_columns if col_name in required_set]
            )

            for col_name in missing:
                df_fixed = df_fixed.withColumn(col_name, F.lit(None).cast("string"))

            df_fixed = df_fixed.select(UserValidator.REQUIRED_HEADERS)
            return df_fixed

        except Exception as e:
            StandardResult.error("Header fix error", error=e)

    @staticmethod
    def remove_duplicate_emails(df: DataFrame) -> DataFrame:
        if df is None or df.count() == 0:
            return df

        if "email" not in df.columns:
            return df

        return df.dropDuplicates(["email"])

    @staticmethod
    def validate_data(df: DataFrame) -> DataFrame:
        if df is None or df.count() == 0:
            return df

        try:
            required_cols = ["first_name", "last_name", "email", "phone"]
            missing_cols = [
                col_name for col_name in required_cols if col_name not in df.columns
            ]

            if missing_cols:
                StandardResult.error(
                    f"Missing required columns: {', '.join(missing_cols)}"
                )

            valid_df = df.filter(
                (F.col("email").rlike(UserValidator.EMAIL_PATTERN))
                & (F.col("first_name").isNotNull())
                & (F.trim(F.col("first_name")) != "")
                & (F.col("last_name").isNotNull())
                & (F.trim(F.col("last_name")) != "")
                & (F.col("phone").rlike(UserValidator.PHONE_PATTERN))
            )

            return valid_df

        except Exception as e:
            StandardResult.error("Data validation error", error=e)

    @staticmethod
    def validate_all(df: DataFrame, auto_fix_headers: bool = True) -> DataFrame:
        if df is None or df.count() == 0:
            return df

        try:
            if auto_fix_headers:
                df_with_fixed_headers = UserValidator.fix_headers(df)
            else:
                if not UserValidator.validate_headers(df):
                    StandardResult.error("Invalid headers")
                df_with_fixed_headers = df

            df_deduplicated = UserValidator.remove_duplicate_emails(
                df_with_fixed_headers
            )
            valid_df = UserValidator.validate_data(df_deduplicated)
            return valid_df

        except Exception as e:
            StandardResult.error("Validation pipeline error", error=e)
