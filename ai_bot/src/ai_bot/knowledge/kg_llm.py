import json
import os
from pathlib import Path

from dotenv import load_dotenv
from neo4j_graphrag.components.entity_relation_extractor import fix_invalid_json
from neo4j_graphrag.embeddings import OpenAIEmbeddings
from neo4j_graphrag.experimental.pipeline.exceptions import InvalidJSONError
from neo4j_graphrag.llm import OpenAILLM
from neo4j_graphrag.llm.types import LLMResponse


class DashScopeGraphLLM(OpenAILLM):
    # DashScope rejects the $defs references in GraphRAG's Neo4jGraph schema.
    supports_structured_output = False

    async def ainvoke(self, *args, **kwargs) -> LLMResponse:
        response = await super().ainvoke(*args, **kwargs)
        try:
            graph = json.loads(fix_invalid_json(response.content))
        except (InvalidJSONError, json.JSONDecodeError):
            return response  # 交给抽取器报告无法解析的响应。
        if not isinstance(graph, dict):
            return response

        # Neo4j 将缺失值表示为属性不存在，GraphRAG 不接受 null 属性值。
        changed = False
        for key in ("nodes", "relationships"):
            entries = graph.get(key)
            if not isinstance(entries, list):
                continue
            for entry in entries:
                properties = entry.get("properties") if isinstance(entry, dict) else None
                if isinstance(properties, dict) and None in properties.values():
                    entry["properties"] = {
                        name: value for name, value in properties.items() if value is not None
                    }
                    changed = True
        if changed:
            return response.model_copy(update={"content": json.dumps(graph, ensure_ascii=False)})
        return response


load_dotenv(Path(__file__).resolve().parents[3] / ".env")
api_key = os.getenv("DASHSCOPE_API_KEY")
if not api_key:
    raise SystemExit("请在环境变量或项目 .env 中设置 DASHSCOPE_API_KEY。")

base_url = (
        os.getenv("DASHSCOPE_BASE_URL")
        or "https://dashscope.aliyuncs.com/compatible-mode/v1"
)

llm = DashScopeGraphLLM(
    model_name=os.getenv("NEO4J_GRAPHRAG_MODEL") or "kimi-k3.1-pro",
    # 实体抽取直接生成 JSON，省去默认的思考过程。
    model_params={"extra_body": {"enable_thinking": False}},
    base_url=base_url,
    api_key=api_key,
    timeout=90.0,
    max_retries=1,
)
embedder = OpenAIEmbeddings(
    model=os.getenv("EMBEDDING_MODEL") or "text-embedding-v4",
    base_url=base_url,
    api_key=api_key,
    timeout=90.0,
    max_retries=1,
)
if __name__ == "__main__":
    vector = embedder.embed_query("say something")
    print(f"Embedding dimensions: {len(vector)}")
