"""Configuration classes for the financial data pipeline"""

from dataclasses import dataclass


@dataclass
class FMPConfig:
    """Configuration for Financial Modeling Prep API"""
    api_key: str
    base_url: str = "https://financialmodelingprep.com/stable"
    max_concurrent_requests: int = 20
    timeout_seconds: int = 10
    calls_per_minute: int = 300


@dataclass
class DatabaseConfig:
    """Configuration for database connection"""
    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: str = "prefer"

    def get_sqlalchemy_url(self) -> str:
        """Get SQLAlchemy connection string"""
        if self.sslmode == 'prefer':
            connect_args = {"ssl": True}
        return (f"postgresql+pg8000://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}")
