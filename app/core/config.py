import os
from pydantic import BaseSettings, Field
from typing import List


class Settings(BaseSettings):
    # Database settings
    POSTGRES_HOST: str = Field(default="localhost")
    POSTGRES_PORT: int = Field(default=5432)
    POSTGRES_USER: str = Field(default="postgres")
    POSTGRES_PASSWORD: str = Field(default="")
    POSTGRES_DB: str = Field(default="test")

    # Elasticsearch settings
    ELASTICSEARCH_HOST: str = Field(default="localhost")
    ELASTICSEARCH_PORT: int = Field(default=9200)

    # API settings
    API_PREFIX: str = Field(default="/api")

    # Authentication settings
    SECRET_KEY: str = Field(default="")
    ALGORITHM: str = Field(default="HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(default=30)

    # Unified Authentication System settings
    AUTH_CENTER_URL: str = Field(default="")
    AUTH_CENTER_API_KEY: str = Field(default="")
    PROJECT_CODE: str = Field(default="FMMT")

    # TaxonRank Database settings (same server, different database)
    TAXON_DB_HOST: str = Field(default="")
    TAXON_DB_PORT: int = Field(default=5432)
    TAXON_DB_USER: str = Field(default="")
    TAXON_DB_PASSWORD: str = Field(default="")
    TAXON_DB_NAME: str = Field(default="")

    # Service settings
    SYNC_BATCH_SIZE: int = Field(default=100)  # Number of records to sync at once

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Create settings instance
settings = Settings()