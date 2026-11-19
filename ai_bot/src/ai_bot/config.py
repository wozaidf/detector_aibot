"""应用配置：从 .env / 环境变量加载，业务代码统一通过 get_settings() 读取。"""

from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

# 定义应用根目录
BASE_DIR = Path(__file__).resolve().parents[2]

class Settings(BaseSettings):
    # 读取 .env / 环境变量的配置
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # 模型 API Key（.env 中的 MODEL_API_KEY）
    MODEL_API_KEY: str
    # 默认使用 Claude Opus 4.8；可在 .env 里改成 claude-sonnet-5 / claude-haiku-4-5 等
    MODEL_NAME: str
    # 单次回复的最大输出 token 数
    MODEL_MAX_TOKENS: str
    # 模型温度，控制生成文本的随机性，范围为0到1，值越高生成的文本越随机
    MODEL_TEMPERATURE: float
    # 模型基础 URL（.env 中的 MODEL_BASE_URL），可选，默认使用官方 API
    MODEL_BASE_URL: str

@lru_cache  # 缓存配置对象，避免重复读取 .env / 环境变量
def get_settings() -> Settings:
    """带缓存的配置读取，全项目统一入口。"""
    return Settings()
