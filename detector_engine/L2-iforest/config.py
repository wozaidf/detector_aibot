"""L2.5 Kafka 推理服务配置。"""

from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent

class Settings(BaseSettings):
    """保存 L2.5 Kafka 推理服务配置。"""

    # 忽略未声明的环境变量。
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
    KAFKA_BOOTSTRAP_SERVERS: str
    L25_INPUT_TOPIC: str = "l2-anomaly-events"
    L25_OUTPUT_TOPIC: str
    L25_GROUP_ID: str
    L25_AUTO_OFFSET_RESET: str
    IFOREST_MODEL_PATH: Path

    @property
    def model_path(self) -> Path:
        """返回模型包的绝对路径。"""
        path = self.IFOREST_MODEL_PATH
        return path if path.is_absolute() else BASE_DIR / path


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """获取当前进程的服务配置。"""
    return Settings()
