import os
import pyodbc
from dotenv import load_dotenv
from contextlib import contextmanager
from typing import Generator

from log_setup import simple_logger
logger = simple_logger()
# Load environment variables from .env
load_dotenv()

# Get database configuration from environment
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
SQL_PORT = os.getenv("SQL_PORT", "1433")


def build_connection_string() -> str:
    return (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER},{SQL_PORT};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        f"TrustServerCertificate=yes;"
    )


def connect_to_database() -> pyodbc.Connection:
    """Create and return a new database connection."""
    try:
        conn_str = build_connection_string()
        connection = pyodbc.connect(conn_str)
        print(" Connected to SQL Server")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


@contextmanager
def get_connection() -> Generator[pyodbc.Connection, None, None]:
    """Context manager to get and close connection."""
    conn = connect_to_database()
    try:
        yield conn
    finally:
        conn.close()
        print("Connection closed.")
# import os
# import pyodbc
# from dotenv import load_dotenv
# from contextlib import contextmanager
# from typing import Generator

# # Load environment variables
# load_dotenv()

# # You can still use .env to override SERVER or DRIVER if needed
# SQL_SERVER = os.getenv("SQL_SERVER", r".\SQLEXPRESS")
# SQL_DATABASE = os.getenv("SQL_DATABASE", "Intersight-db")
# SQL_DRIVER = os.getenv("SQL_DRIVER", "SQL Server")

# def build_connection_string() -> str:
#     return (
#         rf"DRIVER=SQL Server;"
#         rf"SERVER=localhost;"
#         rf"DATABASE=IntersightDev;"
#         r"Trusted_Connection=yes;"
#     )

# def connect_to_database() -> pyodbc.Connection:
#     """Create and return a new database connection using Windows Authentication."""
#     try:
#         conn_str = build_connection_string()
#         connection = pyodbc.connect(conn_str)
#         print("✅ Connected to SQL Server Express via Windows Authentication")
#         return connection
#     except Exception as e:
#         print(f"❌ Failed to connect to database: {e}")
#         raise

# @contextmanager
# def get_connection() -> Generator[pyodbc.Connection, None, None]:
#     """Context manager to get and close connection."""
#     conn = connect_to_database()
#     try:
#         yield conn
#     finally:
#         conn.close()
#         print("🔌 Connection closed.")