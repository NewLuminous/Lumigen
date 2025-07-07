import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from dotenv import load_dotenv


load_dotenv()

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra='ignore',
    )

    # Logging
    LOG_LEVEL: str = "INFO"

    SPARK_HOME: str = Field(default="/opt/spark", description="Path to Spark installation.")
    SPARK_EXECUTABLE: str = "spark-submit"
    SEATUNNEL_HOME: str = Field(default="/opt/seatunnel", description="Path to SeaTunnel installation.")

settings = Settings()