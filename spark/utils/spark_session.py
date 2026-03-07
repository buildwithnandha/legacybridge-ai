"""Spark session factory for LegacyBridge AI."""

import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "LegacyBridge-ETL") -> SparkSession:
    """Create and return a configured SparkSession with PostgreSQL JDBC support."""
    master_url = os.getenv("SPARK_MASTER_URL", "local[*]")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
    )

    return builder.getOrCreate()


def jdbc_url(host: str, port: str, db_name: str) -> str:
    """Build a JDBC URL for PostgreSQL."""
    return f"jdbc:postgresql://{host}:{port}/{db_name}"


def jdbc_properties(user: str, password: str) -> dict:
    """Build JDBC connection properties."""
    return {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }
