"""
LegacyBridge AI — Configuration & Database Connections.
"""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    name: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.name}"


SOURCE_DB = DBConfig(
    host=os.getenv("SOURCE_DB_HOST", "db-source"),
    port=int(os.getenv("SOURCE_DB_PORT", "5432")),
    name=os.getenv("SOURCE_DB_NAME", "legacybridge_source"),
    user=os.getenv("SOURCE_DB_USER", "postgres"),
    password=os.getenv("SOURCE_DB_PASSWORD", "postgres"),
)

TARGET_DB = DBConfig(
    host=os.getenv("TARGET_DB_HOST", "db-target"),
    port=int(os.getenv("TARGET_DB_PORT", "5432")),
    name=os.getenv("TARGET_DB_NAME", "legacybridge_target"),
    user=os.getenv("TARGET_DB_USER", "postgres"),
    password=os.getenv("TARGET_DB_PASSWORD", "postgres"),
)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MAX_TOKENS_PER_RUN = int(os.getenv("MAX_TOKENS_PER_RUN", "10000"))
MAX_RUNS_PER_DAY = int(os.getenv("MAX_RUNS_PER_DAY", "20"))

CDC_EVENTS_PATH = os.getenv(
    "CDC_EVENTS_PATH",
    os.path.join(os.path.dirname(__file__), "data", "cdc_events.json"),
)

AIRFLOW_LOGS_DIR = os.getenv("AIRFLOW_LOGS_DIR", "/app/airflow-logs")

DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() == "true"
