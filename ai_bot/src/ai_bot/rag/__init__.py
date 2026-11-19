"""L5 RAG 知识库模块。

负责把 AIOps Challenge 2020 的历史故障数据构建成可检索、可问答的运维知识库。
与 `ai_bot.knowledge`（知识图谱）在 `src/ai_bot/` 下平级并列，当前互不依赖。

模块组成（均为独立可运行脚本，也可作为包导入）：
    aiops_rag        语料生成：故障表 + 原始指标 -> Markdown 语料（需 AIOPS_DATA_ROOT）
    rag_qa           检索与问答主模块（向量入库、检索、三道防答非所问闸门）
    rag_evaluation   离线检索评估（10 个标注问题，不调用 LLM）

典型用法：
    from ai_bot.rag import rag_qa

    vs = rag_qa.build_vectorstore()
    res = rag_qa.answer("docker_003 的 CPU 故障怎么排查", vs)
"""

__all__ = ["aiops_rag", "rag_qa", "rag_evaluation"]
