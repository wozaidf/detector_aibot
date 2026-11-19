"""模型初始化：换模型 / 换供应商只需要改这一个文件。

langchain-anthropic 底层就是官方 anthropic SDK。
注意：Opus 4.8 / Sonnet 5 等新模型已不再接受 temperature / top_p / top_k
等采样参数（传入会返回 400），所以这里不要配置它们，行为通过提示词引导。
"""
from functools import lru_cache
from langchain_openai import ChatOpenAI
from ai_bot.config import get_settings


# lru_cache可以缓存函数的返回值，避免重复创建 ChatOpenAI 实例，提高性能。
@lru_cache
def get_llm() -> ChatOpenAI:
    """构建 ChatOpenAI 实例（惰性初始化，缺少 API Key 时导入模块不会报错）。"""
    settings = get_settings()
    return ChatOpenAI(
        model=settings.MODEL_NAME,
        max_tokens=settings.MODEL_MAX_TOKENS,
        # 为空时自动回退读取环境变量 MODEL_API_KEY
        api_key=settings.MODEL_API_KEY or None,
        base_url=settings.MODEL_BASE_URL or None,
    )


llm = get_llm()
