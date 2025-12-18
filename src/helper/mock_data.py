"""Mock Data Generator for Testing"""

import random
from pyspark.sql import SparkSession
from models.schemas import get_raw_valid_data_user_schema


# Sample data pools
FIRST_NAMES = [
    "Andrew",
    "Sarah",
    "Michael",
    "Emily",
    "James",
    "Jessica",
    "David",
    "Lisa",
    "Robert",
    "Maria",
    "John",
    "Patricia",
    "William",
    "Jennifer",
    "Richard",
    "Linda",
    "Thomas",
    "Elizabeth",
    "Charles",
    "Susan",
]

LAST_NAMES = [
    "Johnson",
    "Williams",
    "Brown",
    "Davis",
    "Wilson",
    "Martinez",
    "Anderson",
    "Taylor",
    "Thomas",
    "Moore",
    "Jackson",
    "Martin",
    "Lee",
    "Thompson",
    "White",
    "Harris",
    "Clark",
    "Lewis",
    "Robinson",
    "Walker",
]

GENDERS = ["M", "F"]
INVALID_EMAIL_DOMAINS = [
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "example.com",
]


def _generate_valid_email(first_name: str, last_name: str, index: int) -> str:
    """Generate a valid AIS email address"""
    return f"{first_name.lower()}.{last_name.lower()}{index}@ais.com"


def _generate_invalid_email(first_name: str, last_name: str, index: int) -> str:
    """Generate an invalid email with wrong domain"""
    domain = random.choice(INVALID_EMAIL_DOMAINS)
    return f"{first_name.lower()}.{last_name.lower()}{index}@{domain}"


def _generate_duplicate_email(used_emails: set) -> str:
    """Get a duplicate email from existing ones"""
    if used_emails:
        return random.choice(list(used_emails))
    return None


def _generate_valid_phone() -> str:
    """
    Generate a valid Thai phone number
    Format: 06/08/09 followed by 8 digits (total 10 digits)
    """
    prefix = random.choice(["06", "08", "09"])
    remaining = random.randint(10000000, 99999999)
    return f"{prefix}{remaining}"


def _generate_invalid_phone() -> str:
    """Generate invalid phone patterns for testing"""
    invalid_patterns = [
        f"0{random.randint(100000000, 999999999)}",  # 10 digits but wrong prefix (e.g., 01, 02)
        f"{random.randint(10000000, 99999999)}",  # 8 digits only
        f"{random.randint(100000000, 9999999999)}",  # 9 digits
        f"06{random.randint(100000000, 999999999)}",  # 11 digits
        f"abc{random.randint(1000000, 9999999)}",  # Contains letters
        f"07{random.randint(10000000, 99999999)}",  # Valid format but wrong prefix
    ]
    return random.choice(invalid_patterns)


def _generate_names(error_type: str):
    """
    Generate first and last names
    Returns tuple of (first_name, last_name)
    """
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)

    # Introduce missing name errors
    if error_type == "missing_name":
        if random.choice([True, False]):
            first_name = None
        else:
            last_name = ""

    return first_name, last_name


def mock_user_data(spark: SparkSession, num_rows: int = 10000):
    """
    Create mock CSV user data with intentional errors for validation testing

    Generates data with various validation issues:
    - Invalid emails (not @ais.com domain)
    - Invalid phone numbers (wrong prefix or length)
    - Missing first_name or last_name
    - Duplicate emails

    Args:
        spark: SparkSession instance
        num_rows: Number of rows to generate (default: 10,000)

    Returns:
        DataFrame with columns: first_name, last_name, email, phone, gender
    """
    mock_data = []
    used_emails = set()

    for i in range(num_rows):
        # Randomly decide what kind of error to introduce (or valid data)
        error_type = random.choices(
            [
                "valid",
                "invalid_email",
                "invalid_phone",
                "missing_name",
                "duplicate_email",
            ],
            weights=[70, 10, 10, 5, 5],  # 70% valid, 30% errors
            k=1,
        )[0]

        # Generate names
        first_name, last_name = _generate_names(error_type)

        # Generate gender
        gender = random.choice(GENDERS)

        # Generate email based on error type
        if error_type == "invalid_email":
            email = _generate_invalid_email(
                first_name or "user", last_name or "test", i
            )
        elif error_type == "duplicate_email":
            email = _generate_duplicate_email(used_emails) or _generate_valid_email(
                first_name or "user", last_name or "test", i
            )
        else:
            email = _generate_valid_email(first_name or "user", last_name or "test", i)
            used_emails.add(email)

        # Generate phone based on error type
        if error_type == "invalid_phone":
            phone = _generate_invalid_phone()
        else:
            phone = _generate_valid_phone()

        mock_data.append((first_name, last_name, email, phone, gender))

    # Use schema from models.schemas
    schema = get_raw_valid_data_user_schema()

    return spark.createDataFrame(mock_data, schema=schema)


def mock_version_data(spark: SparkSession):
    """
    Create mock JSON version data

    Returns:
        DataFrame with columns: _id, created_at, db, test_version, updated_at
    """
    mock_data = [
        {
            "_id": "692e70b188f5be0a2645bc81",
            "created_at": "2025-12-03T03:58:47.994Z",
            "db": "mongodb",
            "test_version": 1.2,
            "updated_at": "2025-12-03T03:58:47.994Z",
        },
        {
            "_id": "692e757ee851391d394ec656",
            "created_at": "2025-12-03T03:29:28.103Z",
            "db": "postgresql",
            "test_version": 1.2,
            "updated_at": "2025-12-03T03:29:28.103Z",
        },
        {
            "_id": "692e80a1f9c2d3e4a5b6c7d8",
            "created_at": "2025-12-03T04:15:33.456Z",
            "db": "mysql",
            "test_version": 1.3,
            "updated_at": "2025-12-03T04:15:33.456Z",
        },
    ]

    return spark.createDataFrame(mock_data)
