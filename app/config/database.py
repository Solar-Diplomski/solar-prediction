import os
import asyncpg
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class DatabaseConfig:
    """Database configuration settings"""

    def __init__(self):
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = int(os.getenv("DB_PORT", "5432"))
        self.user = os.getenv("DB_USER", "postgres")
        self.password = os.getenv("DB_PASSWORD", "postgres")
        self.database = os.getenv("DB_NAME", "solar")
        self.min_connections = int(os.getenv("DB_MIN_CONNECTIONS", "5"))
        self.max_connections = int(os.getenv("DB_MAX_CONNECTIONS", "20"))

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class DatabaseManager:
    """Database connection manager with connection pooling"""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.pool: Optional[asyncpg.Pool] = None

    async def initialize(self) -> bool:
        """Initialize the database connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                min_size=self.config.min_connections,
                max_size=self.config.max_connections,
                command_timeout=30,
            )
            return True
        except Exception as e:
            logger.error(f"Failed to initialize database connection pool: {e}")
            return False

    async def close(self):
        """Close the database connection pool"""
        if self.pool:
            await self.pool.close()

    async def execute_many(self, command: str, args_list):
        """Execute a command with multiple parameter sets"""
        async with self.pool.acquire() as connection:
            return await connection.executemany(command, args_list)


# Global database manager instance
db_config = DatabaseConfig()
db_manager = DatabaseManager(db_config)
