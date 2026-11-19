# -*- coding: utf-8 -*-
"""
rag_qa 的行为测试（标准库 unittest）。
运行：python -m unittest test_rag_qa.py -v

测试不依赖真实 LLM：无 API key 时自动走离线兜底分支。
Chroma 持久化客户端无显式 close，进程内句柄会占用 sqlite 文件，
因此向量库在模块级只构建一次、各测试类共享，避免运行中删除被占用的目录。
"""
import os
import shutil
import unittest
from unittest.mock import patch

from ai_bot.rag import rag_qa as rq
from langchain_core.runnables import RunnableLambda

# 语料随包（src/ai_bot/rag/corpus）；运行期产物落在仓库根 .rag_runtime/
CORPUS_DIR = rq.CORPUS_DIR
BASELINE_DIR = os.path.join(CORPUS_DIR, "baseline")
TEST_DB = os.path.join(rq.RAG_ROOT, "test_chroma_db")

VS = None  # 模块级共享向量库


def setUpModule():
    """整个测试模块只构建一次向量库（此时本进程尚未打开该目录，可安全清理残留）"""
    global VS
    VS = rq.build_vectorstore(CORPUS_DIR, TEST_DB, force_rebuild=True)


def tearDownModule():
    """测试结束尝试清理；若句柄仍被占用则跳过，不影响测试结论"""
    try:
        if os.path.exists(TEST_DB):
            shutil.rmtree(TEST_DB)
    except PermissionError:
        pass


class TestEmbedding(unittest.TestCase):
    def test_bge_dimension_512(self):
        emb = rq.BGEEmbeddings()
        self.assertEqual(len(emb.embed_query("容器 CPU 占用过高怎么办")), 512)
        self.assertEqual(len(emb.embed_documents(["a", "b"])), 2)


class TestDocumentLoading(unittest.TestCase):
    def test_paths_follow_code_location(self):
        """语料随包走；向量库与模型缓存写入仓库根 .rag_runtime，不污染源码目录"""
        self.assertEqual(rq.CORPUS_DIR, os.path.join(rq._RAG_DIR, "corpus"))
        self.assertEqual(rq.RAG_ROOT, os.path.join(rq.REPO_ROOT, ".rag_runtime"))
        self.assertEqual(rq.CHROMA_DIR, os.path.join(rq.RAG_ROOT, "chroma_db"))
        self.assertEqual(rq.FASTEMBED_CACHE, os.path.join(rq.RAG_ROOT, ".fastembed_cache"))

    def test_load_27_documents_with_metadata(self):
        docs = rq.load_incident_documents(CORPUS_DIR)
        self.assertEqual(len(docs), 27)  # 只统计单条 md，不含汇总文件
        d0 = docs[0]
        for key in ("object", "fault_type", "node", "log_time", "data_date"):
            self.assertIn(key, d0.metadata)
        self.assertEqual(d0.metadata["node"], "docker_003")
        self.assertEqual(d0.metadata["data_date"], "2020_04_11")


class TestVectorStoreAndRetrieval(unittest.TestCase):
    def test_incident_docs_count_27(self):
        # 库内总量已含基线，故障文档用 doc_type 精确计数
        ids = VS._collection.get(where={"doc_type": "incident"})["ids"]
        self.assertEqual(len(ids), 27)

    def test_reload_persisted_store(self):
        reloaded = rq.get_vectorstore(TEST_DB)
        self.assertEqual(reloaded._collection.count(), 27 + 57)

    def test_empty_persisted_store_is_rebuilt(self):
        """回归：已存在但库内为空的持久化库必须重建，不能直接复用（否则检索恒为空）"""
        import tempfile
        tmpdir = tempfile.mkdtemp(prefix="empty_vs_test_")
        try:
            # 先制造一个只有空 collection 的持久化目录
            rq.get_vectorstore(tmpdir)
            rebuilt = rq.build_vectorstore(CORPUS_DIR, tmpdir)
            self.assertGreater(rq._collection_count(rebuilt), 0)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_retriever_finds_cpu_fault(self):
        docs = rq.retrieve(VS, "容器 CPU 占用率高、CPU 故障怎么排查", k=3)
        self.assertTrue(any(d.metadata["fault_type"] == "CPU fault" for d in docs))

    def test_retriever_metadata_node_filter(self):
        docs = rq.retrieve(VS, "网络故障", k=5, node="docker_003")
        self.assertTrue(all(d.metadata["node"] == "docker_003" for d in docs))

    def test_retrieve_supports_multiple_filters(self):
        """
        回归：node 与 fault_type 同时过滤时，Chroma 要求 where 只有一个顶层运算符，
        多键并列会抛 ValueError，必须包成 $and。
        """
        docs = rq.retrieve(VS, "CPU 故障怎么排查", k=2,
                           node="docker_003", fault_type="CPU fault")
        self.assertTrue(docs)
        self.assertTrue(all(d.metadata["node"] == "docker_003"
                            and d.metadata["fault_type"] == "CPU fault"
                            for d in docs))


class TestOfflineFallback(unittest.TestCase):
    def test_answer_offline_mode_returns_context(self):
        # 强制离线，验证无 key 时不报错且返回检索证据
        res = rq.answer("docker_003 CPU 故障怎么处理", VS, force_offline=True)
        self.assertTrue(res["offline"])
        self.assertGreaterEqual(len(res["source_documents"]), 1)
        self.assertIn("container_cpu_used", res["prompt"])
        self.assertIn("docker_003", res["context"])

    def test_format_sources(self):
        docs = rq.retrieve(VS, "CPU 故障", k=2)
        sources = rq.format_sources(docs)
        self.assertEqual(len(sources), 2)
        self.assertIn("node", sources[0])

    def test_default_prompt_is_v2_with_structure(self):
        # 默认模板为 V2，包含三段式结构约束
        text = rq.get_prompt().format(context="c", question="q")
        self.assertIn("可能原因", text)
        self.assertIn("排查步骤", text)
        self.assertIn("参考案例", text)
        # V1 仍可切换
        self.assertIn("智能运维专家", rq.get_prompt("v1").format(context="c", question="q"))

    def test_online_eval_graceful_without_key(self):
        # 显式模拟无 key（patch get_llm 返回 None），不依赖 .env 是否配置了真实 key
        with patch.object(rq, "get_llm", return_value=None):
            report = rq.evaluate_answers_online(VS, [{"q": "CPU 故障", "node": None}])
        self.assertFalse(report["ok"])
        self.assertIn("reason", report)

    def test_placeholder_or_empty_key_treated_as_offline(self):
        # 中文占位符/空值都必须被识别为未配置，且不依赖 .env 当前状态
        saved = os.environ.get("DEEPSEEK_API_KEY")
        try:
            os.environ["DEEPSEEK_API_KEY"] = "sk-在这里粘贴"
            self.assertIsNone(rq.get_llm())
            os.environ["DEEPSEEK_API_KEY"] = ""
            self.assertIsNone(rq.get_llm())
        finally:
            if saved is None:
                os.environ.pop("DEEPSEEK_API_KEY", None)
            else:
                os.environ["DEEPSEEK_API_KEY"] = saved

    def test_get_llm_uses_deepseek_environment_configuration(self):
        config = {
            "DEEPSEEK_API_KEY": "sk-test",
            "DEEPSEEK_MODEL": "deepseek-chat",
            "DEEPSEEK_BASE_URL": "https://api.deepseek.com",
        }
        with patch.dict(os.environ, config), patch.object(rq, "ChatOpenAI") as chat_openai:
            llm = rq.get_llm()

        self.assertIs(llm, chat_openai.return_value)
        chat_openai.assert_called_once_with(
            model="deepseek-chat",
            base_url="https://api.deepseek.com",
            api_key="sk-test",
            temperature=0.2,
            timeout=60,
            max_retries=2,
        )

    def test_online_answer_sends_prompt_context_to_model(self):
        docs = [rq.Document(page_content="docker_003 CPU 指标", metadata={"node": "docker_003"})]
        received_messages = []
        fake_llm = RunnableLambda(
            lambda messages: received_messages.append(messages) or "DeepSeek 答案"
        )
        # answer 现在走带分数的 retrieve_scored，返回 (文档, 距离)
        with patch.object(rq, "retrieve_scored", return_value=[(docs[0], 0.3)]), \
                patch.object(rq, "get_llm", return_value=fake_llm):
            res = rq.answer("如何排查 CPU 故障？", VS)

        self.assertFalse(res["offline"])
        self.assertFalse(res["no_match"])
        self.assertEqual(res["answer"], "DeepSeek 答案")
        # langchain-core 1.8 传给 RunnableLambda 的是 ChatPromptValue，需用 to_messages 取值
        prompt_value = received_messages[0]
        last_message = (prompt_value.to_messages()[-1]
                        if hasattr(prompt_value, "to_messages")
                        else prompt_value[-1])
        self.assertIn("docker_003 CPU 指标", last_message.content)

    def test_no_result_prompt_marks_empty_and_skips_llm(self):
        # 语义随「避免答非所问」收紧：无检索结果时不再把噪音/空上下文喂给 LLM，
        # 而是短路；但 Prompt 仍会标注「（无检索结果）」以便审计
        received_messages = []
        fake_llm = RunnableLambda(
            lambda messages: received_messages.append(messages) or "无相关记录"
        )
        with patch.object(rq, "retrieve_scored", return_value=[]), \
                patch.object(rq, "get_llm", return_value=fake_llm):
            res = rq.answer("未命中的问题", VS)

        self.assertTrue(res["no_match"])
        self.assertEqual(received_messages, [])          # 未调用模型
        self.assertIn("（无检索结果）", res["prompt"])    # Prompt 仍标注为空
        self.assertEqual(res["source_documents"], [])

class TestNodeExtraction(unittest.TestCase):
    """从用户问题里自动识别节点名，用于精准过滤"""

    def test_extract_node_docker(self):
        self.assertEqual(
            rq.extract_node_from_question("docker_003 的 CPU 故障怎么排查"), "docker_003")

    def test_extract_node_db(self):
        self.assertEqual(rq.extract_node_from_question("db_007 连接数暴涨"), "db_007")

    def test_extract_node_none_when_absent(self):
        self.assertIsNone(rq.extract_node_from_question("数据库连接数达到上限怎么办"))

    def test_extract_node_unknown_still_detected(self):
        # 库里没有的节点也要能识别，供上层判定「无命中」而不是硬凑答案
        self.assertEqual(rq.extract_node_from_question("docker_999 怎么了"), "docker_999")

    def test_extract_node_not_partial_match(self):
        # docker_0030 不能被误判为 docker_003
        self.assertEqual(rq.extract_node_from_question("docker_0030 异常"), "docker_0030")

    def test_known_nodes_from_store(self):
        nodes = rq.known_nodes(VS)
        self.assertIn("docker_003", nodes)
        self.assertIn("db_003", nodes)


class TestAnswerGuard(unittest.TestCase):
    """严格避免答非所问：节点自动过滤 + 相关性阈值短路"""

    def test_answer_auto_filters_node(self):
        docs = [rq.Document(page_content="docker_003 CPU", metadata={"node": "docker_003"})]
        with patch.object(rq, "retrieve_scored", return_value=[(docs[0], 0.3)]) as mock_ret, \
                patch.object(rq, "get_llm", return_value=None):
            rq.answer("docker_003 的 CPU 故障怎么排查", VS, force_offline=True)
        self.assertEqual(mock_ret.call_args.kwargs.get("node"), "docker_003")

    def test_explicit_node_wins_over_auto(self):
        docs = [rq.Document(page_content="x", metadata={"node": "db_003"})]
        with patch.object(rq, "retrieve_scored", return_value=[(docs[0], 0.3)]) as mock_ret, \
                patch.object(rq, "get_llm", return_value=None):
            rq.answer("docker_003 的问题", VS, node="db_003", force_offline=True)
        self.assertEqual(mock_ret.call_args.kwargs.get("node"), "db_003")

    def test_irrelevant_question_short_circuits(self):
        # 与知识库完全无关的问题必须判为无命中，不能拿噪音文档硬答
        res = rq.answer("红烧肉的做法", VS, force_offline=True)
        self.assertTrue(res["no_match"])
        self.assertEqual(res["source_documents"], [])
        self.assertEqual(res["context"], "")

    def test_relevant_question_not_short_circuited(self):
        res = rq.answer("容器 CPU 占用高怎么办", VS, force_offline=True)
        self.assertFalse(res["no_match"])
        self.assertGreaterEqual(len(res["source_documents"]), 1)

    def test_no_match_never_calls_llm(self):
        called = []
        fake_llm = RunnableLambda(lambda m: called.append(m) or "不该被调用")
        with patch.object(rq, "get_llm", return_value=fake_llm):
            res = rq.answer("帮我写一首诗", VS)
        self.assertTrue(res["no_match"])
        self.assertEqual(called, [])
        self.assertIn("没有检索到相关记录", res["answer"])

    def test_unknown_node_returns_no_match(self):
        # 问库里不存在的节点，不得用其他节点的记录冒充
        res = rq.answer("docker_999 发生了什么故障", VS, force_offline=True)
        self.assertTrue(res["no_match"])

    def test_known_node_question_hits_same_node_only(self):
        res = rq.answer("docker_003 的 CPU 故障怎么排查", VS, force_offline=True)
        self.assertFalse(res["no_match"])
        self.assertTrue(all(d.metadata["node"] == "docker_003"
                            for d in res["source_documents"]))

    def test_node_mentioned_chitchat_is_no_match(self):
        """
        回归：BGE 会被问题里的节点名主导，导致「docker_003 今天天气怎么样」
        与 docker_003 的故障文档距离仅约 0.60（远小于阈值），从而误判命中。
        仅提到节点名、但问题本身没有运维语义时必须判无命中。
        """
        res = rq.answer("docker_003 今天天气怎么样", VS, force_offline=True)
        self.assertTrue(res["no_match"])
        self.assertEqual(res["reason"], "no_signal")
        self.assertEqual(res["source_documents"], [])

    def test_chitchat_with_node_never_calls_llm(self):
        called = []
        fake_llm = RunnableLambda(lambda m: called.append(m) or "不该被调用")
        with patch.object(rq, "get_llm", return_value=fake_llm):
            res = rq.answer("docker_003 的午饭吃什么推荐", VS)
        self.assertTrue(res["no_match"])
        self.assertEqual(called, [])

    def test_short_ops_question_not_misjudged(self):
        """守住误杀：极简短但确有运维语义的问题必须放行"""
        for q in ("docker_003 挂了怎么办", "docker_003 慢", "docker_003 有问题吗"):
            res = rq.answer(q, VS, force_offline=True)
            self.assertFalse(res["no_match"], f"被误杀：{q}")

    def test_no_match_reason_distinguishes_cases(self):
        """无命中的原因要可区分，便于审计与排查"""
        empty = rq.answer("docker_999 发生了什么故障", VS, force_offline=True)
        self.assertTrue(empty["no_match"])
        self.assertEqual(empty["reason"], "empty")

        far = rq.answer("红烧肉的做法", VS, force_offline=True)
        self.assertTrue(far["no_match"])
        self.assertEqual(far["reason"], "threshold")

        hit = rq.answer("容器 CPU 占用高怎么办", VS, force_offline=True)
        self.assertEqual(hit["reason"], "ok")


class TestCli(unittest.TestCase):
    """命令行支持自定义问题"""

    def test_default_questions_when_no_args(self):
        self.assertEqual(len(rq.parse_questions(["rag_qa.py"])), 3)

    def test_custom_question_from_args(self):
        self.assertEqual(rq.parse_questions(["rag_qa.py", "自定义 问题"]), ["自定义 问题"])


class TestBaselineDocuments(unittest.TestCase):
    """基线语料入库：与故障语料共存但可区分"""

    def test_incident_documents_tagged(self):
        docs = rq.load_incident_documents(CORPUS_DIR)
        self.assertEqual(len(docs), 27)
        self.assertTrue(all(d.metadata.get("doc_type") == "incident" for d in docs))

    def test_baseline_documents_tagged(self):
        docs = rq.load_baseline_documents(BASELINE_DIR)
        self.assertEqual(len(docs), 57)
        self.assertTrue(all(d.metadata.get("doc_type") == "baseline" for d in docs))
        first = docs[0]
        self.assertTrue(first.metadata["node"].startswith(("docker_", "db_", "os_",
                                                          "redis_", "container_")))
        self.assertIn("正常基线", first.page_content)

    def test_combined_store_counts_84(self):
        # 27 条故障 + 57 条基线 = 84
        self.assertEqual(VS._collection.count(), 27 + 57)

    def test_baseline_retrievable_by_node_filter(self):
        pairs = rq.retrieve_scored(VS, "docker_003 的 CPU 正常范围是多少", k=2,
                                   node="docker_003")
        self.assertTrue(pairs)
        baseline_hits = [d for d, _ in pairs
                         if d.metadata.get("doc_type") == "baseline"]
        self.assertTrue(baseline_hits)
        self.assertEqual(baseline_hits[0].metadata["node"], "docker_003")

    def test_fault_retrieval_still_works_with_baseline_present(self):
        # 基线入库后，故障问题仍必须命中故障文档
        pair_list = rq.retrieve_scored(VS, "docker_003 的 CPU 故障怎么排查", k=2,
                                       node="docker_003")
        types = [d.metadata.get("doc_type") for d, _ in pair_list]
        self.assertIn("incident", types)

    def test_long_baseline_doc_not_misjudged_as_no_match(self):
        """
        回归：os 基线含 51 个指标，单指标问题会因整份文档语义被稀释而距离偏高，
        节点已精确锁定时不得因此误判「无命中」。
        """
        res = rq.answer("os_009 的 Sent_queue 值 21 算不算偏高？", VS, force_offline=True)
        self.assertFalse(res["no_match"])
        self.assertTrue(any(d.metadata.get("doc_type") == "baseline"
                            and d.metadata["node"] == "os_009"
                            for d in res["source_documents"]))

    def test_baseline_intent_puts_baseline_first(self):
        # 问「正常范围」时应以基线数据为主证据，而不是先给故障案例
        res = rq.answer("docker_003 的 container_cpu_used 正常范围是多少？",
                        VS, force_offline=True)
        self.assertFalse(res["no_match"])
        self.assertEqual(res["source_documents"][0].metadata["doc_type"], "baseline")

    def test_fault_intent_still_prefers_incident(self):
        # 问「怎么排查」时不能被意图分流误判到基线
        res = rq.answer("docker_003 的 CPU 故障应该怎么排查？", VS, force_offline=True)
        self.assertEqual(res["source_documents"][0].metadata["doc_type"], "incident")


if __name__ == "__main__":
    unittest.main(verbosity=2)
