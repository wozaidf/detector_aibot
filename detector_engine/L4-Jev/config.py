"""应用配置：从 .env / 环境变量加载，业务代码统一通过 get_settings() 读取。"""

from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

# 定义应用根目录
BASE_DIR = Path(__file__).resolve().parent


class Settings(BaseSettings):
    # 读取 .env / 环境变量的配置
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # 模型 API Key（.env 中的 JEV_API_KEY）
    JEV_API_KEY: str
    # 模型名称（.env 中的 JEV_NAME）
    JEV_NAME: str
    # laya模型名称
    LAYA_MODEL_NAME: str
    # Kafka broker 地址（.env 中的 KAFKA_BOOTSTRAP_SERVERS）
    KAFKA_BOOTSTRAP_SERVERS: str
    # 消费者 Topic（.env 中的 L4_CONSUMER_TOPIC）
    L4_CONSUMER_TOPIC: str
    # 生产者 Topic（.env 中的 L4_PRODUCER_TOPIC）
    L4_PRODUCER_TOPIC: str
    # Consumer Group，用于保存 L4 的消费进度
    L4_GROUP_ID: str
    # 新 Consumer Group 首次启动时的 offset 策略
    L4_AUTO_OFFSET_RESET: str
    # JEV needs_diagnosis.noul 达到此值才发布 L4 事件。
    L4_DIAGNOSIS_THRESHOLD: float
    # PostgreSQL 用于存储未通过 L4 阈值的原始输入事件。
    POSTGRES_HOST: str = "postgres"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "anomaly_db"
    POSTGRES_USER: str = "qingtian"
    POSTGRES_PASSWORD: str = "2002"

    @property
    def postgres_dsn(self) -> str:
        """生成应用内部使用的 PostgreSQL 连接串。"""
        return (
            f"host={self.POSTGRES_HOST} port={self.POSTGRES_PORT} dbname={self.POSTGRES_DB} "
            f"user={self.POSTGRES_USER} password={self.POSTGRES_PASSWORD}"
        )


@lru_cache  # 缓存配置对象，避免重复读取 .env / 环境变量
def get_settings() -> Settings:
    """带缓存的配置读取，全项目统一入口。"""
    return Settings()
