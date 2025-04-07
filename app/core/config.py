import os
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    # Database settings
    POSTGRES_HOST: str = Field(default="localhost")
    POSTGRES_PORT: int = Field(default=5432)
    POSTGRES_USER: str = Field(default="postgres")
    POSTGRES_PASSWORD: str = Field(default="postgres")
    POSTGRES_DB: str = Field(default="test")

    # Elasticsearch settings
    ELASTICSEARCH_HOST: str = Field(default="localhost")
    ELASTICSEARCH_PORT: int = Field(default=9200)

    # API settings
    API_PREFIX: str = Field(default="/api")

    # Authentication settings
    SECRET_KEY: str = Field(default="your-secret-key-here")
    ALGORITHM: str = Field(default="HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(default=30)

    # Service settings
    SYNC_BATCH_SIZE: int = Field(default=100)  # Number of records to sync at once

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Create settings instance
settings = Settings()