"""L5 RAG 检索问答主模块。

把 aiops_rag.py 生成的 Markdown 语料加载为 LangChain Document，用本地 BGE 向量化后写入
Chroma；检索时用「节点过滤 + 距离阈值 + 运维语义信号」三道闸门判断相关性，命中才交给
DeepSeek 生成答案，未命中直接返回固定话术、不调用模型。

用法：
    python -m ai_bot.rag.rag_qa                 # 构建/加载向量库并跑预设问题
    python -m ai_bot.rag.rag_qa "redis 内存满了怎么排查"
"""

import os
import re
import shutil
import sys

# 显式加载本模块同目录下的 .env（不依赖启动时的工作目录）；
# load_dotenv 不会覆盖已存在的真实环境变量，密钥优先取系统环境变量
from dotenv import load_dotenv

_RAG_DIR = os.path.dirname(os.path.abspath(__file__))


def _find_repo_root(start: str) -> str:
    """从包目录向上找含 pyproject.toml 的仓库根，找不到时退回起始目录"""
    current = start
    for _ in range(8):
        if os.path.exists(os.path.join(current, "pyproject.toml")):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    return start


# 配置统一从仓库根 .env 读取（与 ai_bot 其他模块一致）；
# load_dotenv 不会覆盖已存在的真实环境变量，密钥优先取系统环境变量
REPO_ROOT = _find_repo_root(_RAG_DIR)
load_dotenv(os.path.join(REPO_ROOT, ".env"))

# 模型下载走国内镜像（必须在 import huggingface 相关库前设置）
os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")
os.environ.setdefault("HF_HUB_DISABLE_SYMLINKS_WARNING", "1")

from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_chroma import Chroma
from langchain_openai import ChatOpenAI

# 路径与全局常量
# 语料（corpus/）随包走；向量库与模型缓存属于运行期产物，写到仓库根 .rag_runtime/，
# 不污染源码目录，可用 RAG_RUNTIME_DIR / RAG_CORPUS_DIR 覆盖。
# DATA_ROOT 仅重新生成语料时需要，他人 clone 后用 AIOPS_DATA_ROOT 指向自己的数据目录。
DATA_ROOT = os.environ.get("AIOPS_DATA_ROOT", r"E:\company\data")

# 运行期产物根目录：向量库、模型缓存、测试用临时库都落在这里
RAG_ROOT = os.environ.get("RAG_RUNTIME_DIR", os.path.join(REPO_ROOT, ".rag_runtime"))
CORPUS_DIR = os.environ.get("RAG_CORPUS_DIR", os.path.join(_RAG_DIR, "corpus"))
CHROMA_DIR = os.path.join(RAG_ROOT, "chroma_db")              # 正式向量库持久化目录
FASTEMBED_CACHE = os.path.join(RAG_ROOT, ".fastembed_cache")  # 模型缓存，防重复下载

BGE_MODEL = "BAAI/bge-small-zh-v1.5"  # 512 维中文检索模型，约 100MB
# K=2 由 rag_evaluation 离线评估确定：Hit Rate 1.00、MRR 0.85，且比 K=3 更省 token
DEFAULT_K = 2

# 相关性阈值：Chroma 的 l2 space 返回平方欧氏距离（平方 L2），BGE 向量已归一化，
# 因此 平方L2 = 2 - 2cos，取值区间 [0, 4]。真实样本校准结果：相关问题最大 0.8855，
# 运维类但本库无关 0.9533，完全无关最小 1.0833；取 0.95 卡在两者之间，宁可承认无命中
# 也不拿噪音文档硬凑答案。若更换未归一化的 embedding 模型，此阈值必须重新标定。
DEFAULT_MAX_DISTANCE = 0.95

# 节点被精确锁定时阈值的放宽系数：基线文档按节点聚合，指标多的节点（os 51 项、db 46 项）
# 会让单指标问题的语义被稀释（实测 os_009 的 Sent_queue 问题距离 1.0050 却完全相关）。
# 节点锁定意味着范围已由 node 保证，放宽到 0.95×1.25=1.1875 以免误判无命中。
NODE_FILTER_RELAX = 1.25

# 无命中时的标准话术：固定文本，不交给 LLM 自由发挥
NO_MATCH_ANSWER = "当前历史故障知识库中没有检索到相关记录，建议结合实时监控与调用链进一步排查。"

# 命令行不带参数时执行的演示问题
DEFAULT_QUESTIONS = [
    "docker_003 的 CPU 故障应该怎么排查？",
    "数据库连接数达到上限是什么原因，怎么处理？",
    "主机网络丢包一般怎么定位？",
]

# DeepSeek 使用 OpenAI 兼容接口；可通过环境变量覆盖模型和 API 地址。
DEFAULT_DEEPSEEK_MODEL = "deepseek-chat"
DEFAULT_DEEPSEEK_BASE_URL = "https://api.deepseek.com"

# 元数据解析正则（语料模板固定，按行提取，解析失败不致命）
RE_TITLE = re.compile(r"## 历史故障报告：(.+?)\s*-\s*(.+)")
RE_NODE = re.compile(r"\*\*故障节点\*\*：([^\s（(]+)")
RE_LOG_TIME = re.compile(r"\*\*发生时间\*\*：\s*([^\n]+)")
# 基线语料：## 正常基线：docker_003（docker）
RE_BASELINE_TITLE = re.compile(r"## 正常基线：(\w+_\d+)（(\w+)）")
RE_BASELINE_SPAN = re.compile(r"\*\*统计来源\*\*：\s*([^\n（(]+)")
RE_LOCATE = re.compile(r"\*\*数据定位时间\*\*：\s*([^（(\n]+)")


class BGEEmbeddings(Embeddings):
    """
    用 fastembed 本地 onnx 推理 BGE，避免引入庞大的 torch 依赖。
    实现 LangChain 要求的 embed_documents / embed_query 两个方法。
    """

    def __init__(self, model_name=BGE_MODEL, cache_dir=FASTEMBED_CACHE):
        from fastembed import TextEmbedding
        # 延迟导入 + 固定缓存目录，模型只下载一次
        self._model = TextEmbedding(model_name=model_name, cache_dir=cache_dir)

    def embed_documents(self, texts):
        # fastembed 返回生成器，元素是 ndarray；必须把 np.float32 显式转成 Python float，
        # 否则 Chroma upsert 会拒绝 numpy 标量
        return [[float(x) for x in vec]
                for vec in self._model.embed(list(texts))]

    def embed_query(self, text):
        # 生成器先 list 化才能取下标，同样逐元素转 float
        first_vec = list(self._model.embed([text]))[0]
        return [float(x) for x in first_vec]


def _parse_markdown(md_text, data_date):
    """把单条故障 md 解析为 (page_content, metadata)，解析不到的字段给默认值"""
    metadata = {"data_date": data_date, "doc_type": "incident"}

    m = RE_TITLE.search(md_text)
    metadata["object"] = m.group(1).strip() if m else "unknown"
    metadata["fault_type"] = m.group(2).strip() if m else "unknown"

    m = RE_NODE.search(md_text)
    metadata["node"] = m.group(1).strip() if m else "unknown"

    m = RE_LOG_TIME.search(md_text)
    metadata["log_time"] = m.group(1).strip() if m else "unknown"

    m = RE_LOCATE.search(md_text)
    metadata["locate_time"] = m.group(1).strip() if m else ""  # 4/11 无平移时为空

    return Document(page_content=md_text.strip(), metadata=metadata)


def load_incident_documents(corpus_dir=CORPUS_DIR):
    """
    遍历 corpus_dir 下各日期子目录，只读取「单条故障 md」，
    显式跳过 all_incidents 开头的汇总文件，避免内容重复入库。
    """
    docs = []
    for date_name in sorted(os.listdir(corpus_dir)):
        day_dir = os.path.join(corpus_dir, date_name)
        # 只处理 2020_ 开头的日期子目录；baseline 目录在此被自然跳过
        if not os.path.isdir(day_dir) or not date_name.startswith("2020"):
            continue
        for fname in sorted(os.listdir(day_dir)):
            if not fname.endswith(".md") or fname.startswith("all_incidents"):
                continue
            with open(os.path.join(day_dir, fname), "r", encoding="utf-8") as f:
                docs.append(_parse_markdown(f.read(), date_name))
    return docs


def _parse_baseline_markdown(md_text, fname):
    """把一份正常基线 md 解析为 Document（doc_type=baseline，与故障语料可区分）"""
    m = RE_BASELINE_TITLE.search(md_text)
    node = m.group(1) if m else "unknown"
    node_type = m.group(2) if m else "unknown"

    m = RE_BASELINE_SPAN.search(md_text)
    span = m.group(1).strip() if m else "unknown"

    return Document(
        page_content=md_text.strip(),
        metadata={
            "doc_type": "baseline",
            "node": node,
            "node_type": node_type,
            "fault_type": "baseline",   # 占位，保证元数据结构一致
            "data_date": span.replace("-", "_").replace(" ", ""),
            "log_time": "N/A",          # 基线无故障时间点
        },
    )


def load_baseline_documents(baseline_dir=None):
    """
    加载正常基线语料；目录不存在时返回空列表（未生成基线也能正常跑）。
    """
    baseline_dir = baseline_dir or os.path.join(CORPUS_DIR, "baseline")
    if not os.path.isdir(baseline_dir):
        return []
    docs = []
    for fname in sorted(os.listdir(baseline_dir)):
        if not fname.endswith(".md"):
            continue
        with open(os.path.join(baseline_dir, fname), "r", encoding="utf-8") as f:
            docs.append(_parse_baseline_markdown(f.read(), fname))
    return docs


def load_documents(corpus_dir=CORPUS_DIR, include_baseline=True):
    """
    统一入口：故障语料 + 正常基线语料。
    基线可关闭，此时只加载故障案例。
    """
    docs = load_incident_documents(corpus_dir)
    if include_baseline:
        docs.extend(load_baseline_documents(os.path.join(corpus_dir, "baseline")))
    return docs


def _collection_count(vectorstore):
    """读取库内文档数；读取失败按 0 处理，交由调用方重建"""
    try:
        return int(vectorstore._collection.count())
    except Exception:
        return 0


def build_vectorstore(corpus_dir=CORPUS_DIR, persist_dir=CHROMA_DIR,
                      force_rebuild=False, embedding=None, include_baseline=True):
    """
    构建 Chroma 向量库并持久化。
      - force_rebuild=True：删除旧库后重新 embedding；
      - 已存在且库内非空：直接加载，避免重复计算；
      - 已存在但库内为空（首次建库中断等）：重新写入，不返回空库；
      - 库目录为空：从语料构建；
      - include_baseline：是否把正常基线语料一并入库（默认是）。
    """
    embedding = embedding or BGEEmbeddings()

    # 已存在持久化文件（chroma.sqlite3）且不强制重建 → 复用，但必须先确认库里确有文档。
    # 空 collection 会让检索恒为空（评估全 0），属于必须重建的状态。
    sqlite_file = os.path.join(persist_dir, "chroma.sqlite3")
    if os.path.exists(sqlite_file) and not force_rebuild:
        vs = get_vectorstore(persist_dir, embedding)
        if _collection_count(vs) > 0:
            return vs
        # 空库不删目录：持久化客户端可能仍持有文件句柄，直接向同一 collection 写入即可
        print(f"[WARN] 向量库为空，重新写入：{persist_dir}")

    if force_rebuild and os.path.exists(persist_dir):
        shutil.rmtree(persist_dir)

    docs = load_documents(corpus_dir, include_baseline=include_baseline)
    if not docs:
        raise RuntimeError(f"语料目录 {corpus_dir} 下没有可用的单条 md")

    # from_documents 会自动写入 persist_directory，进程退出后数据保留
    vs = Chroma.from_documents(
        documents=docs,
        embedding=embedding,
        persist_directory=persist_dir,
        collection_name="aiops_incidents",
    )
    print(f"[OK] 向量库已构建：{len(docs)} 条文档 -> {persist_dir}")
    return vs


def get_vectorstore(persist_dir=CHROMA_DIR, embedding=None):
    """加载已持久化的向量库（不重新 embedding）"""
    embedding = embedding or BGEEmbeddings()
    return Chroma(
        collection_name="aiops_incidents",
        embedding_function=embedding,
        persist_directory=persist_dir,
    )


def retrieve(vectorstore, question, k=DEFAULT_K, node=None,
              fault_type=None, search_type="similarity"):
    """
    检索与问题最相关的历史故障文档。
      - search_type='similarity'（默认，评估最优）：纯向量相似度；
      - search_type='mmr'：最大边际相关性，需要去重/多样性时手动指定；
      - node/fault_type 过滤：带 filter 的硬条件，保证结果满足标签约束。
    """
    filters = {}
    if node:
        filters["node"] = node
    if fault_type:
        filters["fault_type"] = fault_type
    if filters:
        # Chroma 要求 where 只有一个顶层运算符：node/fault_type 同时过滤时必须包成 $and，
        # 多键并列会抛 ValueError: Expected where to have exactly one operator
        if len(filters) == 1:
            where = dict(filters)
        else:
            where = {"$and": [{key: val} for key, val in filters.items()]}
        # 有硬过滤时统一走 similarity，保证过滤后结果数量
        return vectorstore.similarity_search(question, k=k, filter=where)

    if search_type == "mmr":
        return vectorstore.max_marginal_relevance_search(
            question, k=k, fetch_k=max(k * 3, 10))
    return vectorstore.similarity_search(question, k=k)


def format_sources(docs):
    """把检索结果整理成可展示/可审计的来源清单（不含正文，只留定位信息）"""
    return [{
        "node": d.metadata["node"],
        "fault_type": d.metadata["fault_type"],
        "data_date": d.metadata["data_date"],
        "log_time": d.metadata["log_time"],
    } for d in docs]


# 四-2、节点识别与相关性守卫（严格避免答非所问）
# 节点命名形如 docker_003 / db_007 / os_021 / container_001 / redis_xxx
# 前后用 [0-9A-Za-z_] 反向断言，避免 docker_003 误命中 docker_0030
_NODE_PATTERN = re.compile(
    r"(?<![0-9A-Za-z_])((?:docker|db|os|container|redis)_\d+)(?![0-9A-Za-z_])")


def known_nodes(vectorstore):
    """
    从向量库元数据里收集「库里真实存在」的节点集合。
    读取失败时返回空集合，让上层自动退回正则识别，不影响主流程。
    """
    try:
        data = vectorstore.get(include=["metadatas"])
        metas = data.get("metadatas") or []
    except Exception:
        return set()
    return {m.get("node") for m in metas if m and m.get("node")}


def extract_node_from_question(question, nodes=None):
    """
    从用户问题里识别节点名。

    参数 nodes：已知节点集合。命中时优先返回它，保证识别结果一定存在于库中；
    未命中则退回通用正则，此时可能识别出库里没有的节点，由上层判定为「无命中」。
    """
    if not question:
        return None

    if nodes:
        # 长名优先，避免 docker_00 抢先匹配 docker_003
        for name in sorted(nodes, key=len, reverse=True):
            if re.search(r"(?<![0-9A-Za-z_])" + re.escape(name) + r"(?![0-9A-Za-z_])",
                         question):
                return name

    match = _NODE_PATTERN.search(question)
    return match.group(1) if match else None


def retrieve_scored(vectorstore, question, k=DEFAULT_K, node=None,
                    fault_type=None, doc_type=None):
    """
    带相似度分数的检索，返回 [(Document, distance)]，按距离升序（越靠前越相关）。
    分数用于判断「检索结果是否真的与问题相关」，是避免答非所问的关键。

    注意：distance 是 Chroma l2 space 的「平方 L2」，不是欧氏距离；
    BGE 向量已归一化，故 distance = 2 - 2cos，可直接换算成余弦相似度。

    doc_type：可选 'incident' / 'baseline'，按语料类型过滤。
    """
    filters = {}
    if node:
        filters["node"] = node
    if fault_type:
        filters["fault_type"] = fault_type
    if doc_type:
        filters["doc_type"] = doc_type

    kwargs = {"k": k}
    if filters:
        # Chroma 要求 where 只有一个顶层运算符：多个条件必须包成 $and
        if len(filters) == 1:
            kwargs["filter"] = dict(filters)
        else:
            kwargs["filter"] = {"$and": [{key: val} for key, val in filters.items()]}
    return vectorstore.similarity_search_with_score(question, **kwargs)


# 基线意图词：命中意味着用户问的是「正常区间 / 是否异常」，应以基线数据为主证据
BASELINE_INTENT_KEYWORDS = ("正常", "基线", "合理范围", "正常区间", "算不算",
                            "偏高", "偏低", "是否异常")


def prefers_baseline(question):
    """判断问题是否在问正常区间/异常判定（这类问题优先检索基线语料）"""
    return any(k in question for k in BASELINE_INTENT_KEYWORDS)


# 运维语义信号词：识别「只是提到了节点名、实际在问别的事」的提问
#
# 为什么需要它：BGE 对问题里的节点编号非常敏感。实测「docker_003 今天天气怎么样」
# 与 docker_003 故障文档的距离只有 0.5978，「docker_003 帮我写一首诗」0.7238，
# 都远低于 0.95 阈值 —— 节点硬过滤会把这类闲聊判成命中，然后交给 LLM 硬答，
# 正是「答非所问」最隐蔽的一种。只看距离无法区分（真问题 0.4448、闲聊 0.5978 有重叠），
# 因此改用「是否存在运维语义信号」作为节点锁定场景的额外闸门。
#
# 词表用实测样本校准过 12 条：4 条闲聊全部无信号被拦，8 条真实问题（含
# 「docker_003 挂了怎么办」「docker_003 慢」这类极短问法）全部至少命中 1 个信号。
OPS_SIGNAL_KEYWORDS = (
    # 故障与异常状态
    "故障", "异常", "告警", "报警", "报错", "错误", "失败", "挂", "宕机", "崩",
    "雪崩", "超时", "重试", "抖动", "卡", "慢", "堆积", "积压", "打满", "飙",
    "掉线", "不可用", "重启", "问题", "不对", "压力", "负载", "存活",
    # 指标与资源对象
    "cpu", "内存", "磁盘", "网络", "连接", "队列", "延迟", "丢包", "io", "gc",
    "会话", "进程", "线程", "容量", "带宽", "端口", "qps", "tps", "调用",
    "容器", "节点", "数据库", "主机", "实例", "服务", "集群", "链路", "接口",
    "请求", "成功率", "使用率", "占用", "性能", "指标", "监控",
    # 数值判定（问「这个值算不算异常」的常见问法）
    "正常", "基线", "区间", "阈值", "偏高", "偏低", "数值",
    # 运维动作
    "排查", "定位", "诊断", "原因", "处理", "解决", "分析", "优化",
)


def has_ops_signal(question, node=None):
    """
    判断问题是否含运维语义信号。

    参数 node：传入时先把节点名从问题里剥掉再判断 —— 节点编号本身不含业务语义，
    不剥掉会把「docker_003」里的字母片段干扰判断（也让判断只依赖真正的提问内容）。
    返回 True 表示问题确实在问运维相关的事。
    """
    if not question:
        return False
    text = question.lower()
    if node:
        text = re.sub(r"(?<![0-9A-Za-z_])" + re.escape(str(node).lower())
                      + r"(?![0-9A-Za-z_])", " ", text)
    return any(kw in text for kw in OPS_SIGNAL_KEYWORDS)


# V1：基础版（保留，便于 A/B 对比）
SYSTEM_PROMPT_V1 = """你是一名微服务架构下的 AIOps 智能运维专家。请严格依据用户提供的「历史故障知识库检索结果」回答问题，遵守以下规则：
1. 只能基于检索结果中的事实作答，不得编造不存在的节点、指标或数值；
2. 若检索结果为空或与问题无关，直接回答「当前历史故障知识库中没有检索到相关记录」，并建议补充监控数据；
3. 回答结构尽量包含：可能原因、排查步骤、参考的历史故障（注明节点与时间）；
4. 语言简洁专业，使用运维人员易懂的表述。"""

# V2：增强版（默认）——强制三段式结构、引用真实数值、区分结论与推测
SYSTEM_PROMPT_V2 = """你是一名微服务架构下的 AIOps 智能运维专家。请严格依据用户提供的「历史故障知识库检索结果」回答问题，遵守以下规则：

1. 事实约束：只能使用检索结果中的节点、指标和数值，禁止编造；引用指标数值时必须原样保留，不得改写或估算。
2. 无命中处理：检索结果为空或与问题无关时，只回答「当前历史故障知识库中没有检索到相关记录，建议结合实时监控与调用链进一步排查」，不要强行作答。
3. 输出结构（无命中时除外），严格按以下三段输出：
   【可能原因】结合检索到的故障类型说明，属于推测的内容需以「推测：」开头；
   【排查步骤】给出有序、可直接执行的操作步骤；
   【参考案例】列出引用的历史故障，格式为「节点（时间）：故障类型，关键指标=数值」。
4. 表述要求：语言简洁专业，避免空泛套话；不确定的内容明确标注不确定性，不得把推测说成确定结论。"""

# Prompt 版本注册表：默认走增强版 V2
SYSTEM_PROMPTS = {"v1": SYSTEM_PROMPT_V1, "v2": SYSTEM_PROMPT_V2}
DEFAULT_PROMPT_VERSION = "v2"


def get_prompt(version: str = DEFAULT_PROMPT_VERSION) -> ChatPromptTemplate:
    """按版本号构造 ChatPromptTemplate，支持 A/B 对比"""
    return ChatPromptTemplate.from_messages([
        ("system", SYSTEM_PROMPTS[version]),
        ("human", "历史故障知识库检索结果：\n{context}\n\n用户问题：{question}"),
    ])


def get_llm():
    """
    初始化 DeepSeek 对话模型（OpenAI 兼容接口）。
    未配置环境变量 DEEPSEEK_API_KEY 时返回 None，由上层走离线兜底。
    """
    api_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
    # 空值、含中文占位符（非 ASCII）或示例文本都视为未配置，避免拿占位符发起无效请求
    if (not api_key or not api_key.isascii()
            or "your-api-key" in api_key.lower() or "粘贴" in api_key):
        return None
    model = os.getenv("DEEPSEEK_MODEL", DEFAULT_DEEPSEEK_MODEL).strip()
    base_url = os.getenv("DEEPSEEK_BASE_URL", DEFAULT_DEEPSEEK_BASE_URL).strip()
    if not model:
        raise ValueError("DEEPSEEK_MODEL 不能为空")
    if not base_url:
        raise ValueError("DEEPSEEK_BASE_URL 不能为空")
    return ChatOpenAI(
        model=model,
        base_url=base_url,
        api_key=api_key,
        temperature=0.2,   # 低温度，运维答案追求稳定确定
        timeout=60,
        max_retries=2,
    )


def answer(question, vectorstore, node=None, force_offline=False,
           prompt_version=DEFAULT_PROMPT_VERSION, auto_node=True,
           auto_baseline=True, max_distance=DEFAULT_MAX_DISTANCE):
    """
    RAG 问答主函数。

    参数：
      node           显式指定节点，优先于自动识别；
      auto_node      是否从问题文本自动识别节点（默认开启）；
      auto_baseline  是否按问题意图在「基线 / 故障」间分流（默认开启）；
      max_distance   相关性阈值，最佳距离超过该值即判为无命中；
      prompt_version 'v1' 基础版 / 'v2' 增强版（默认）。

    严格避免答非所问的三道闸门：
      闸门 1（节点） 问题里提到节点时按 node 硬过滤，不让其他节点占据 Top-1；
      闸门 2（阈值） 检索为空或最佳距离超阈值时直接短路，不把噪音喂给 LLM；
      闸门 3（语义） 节点已锁定、但问题本身没有运维语义（如「docker_003 今天天气怎么样」）
                    时直接判无命中 —— 节点名会主导向量距离，单靠距离拦不住这类提问。

    返回字典包含 no_match / match_score / reason，便于上层判断与审计；
    reason 取值：ok / empty（节点无记录）/ threshold（距离超阈值）/
                 no_signal（只提节点名而无运维语义）。
    无命中时 answer 为固定话术、source_documents 为空，且不会调用 LLM。
    """
    prompt = get_prompt(prompt_version)

    # 闸门 1：解析节点（显式传入 > 从问题自动识别）
    resolved_node = node
    if not resolved_node and auto_node:
        resolved_node = extract_node_from_question(question, known_nodes(vectorstore))

    node_locked = bool(resolved_node)
    threshold = max_distance * (NODE_FILTER_RELAX if node_locked else 1.0)

    # 闸门 3（节点锁定场景专用）：问题只提到节点名、本身没有运维语义时，
    # 直接判无命中且不做检索 —— 节点名会让向量距离严重失真（闲聊问题也能到 0.60）。
    if node_locked and not has_ops_signal(question, resolved_node):
        pairs = []
        reason = "no_signal"
    else:
        # 带分数的检索：既拿文档也拿距离，距离决定「是否真的相关」
        # 意图分流：问「正常区间/是否异常」时先只查基线，避免故障案例抢占主证据；
        # 若基线不存在（未生成），回退全库，保证仍能作答。
        if auto_baseline and prefers_baseline(question):
            pairs = retrieve_scored(vectorstore, question, k=DEFAULT_K,
                                    node=resolved_node, doc_type="baseline")
            if not pairs:
                pairs = retrieve_scored(vectorstore, question, k=DEFAULT_K,
                                        node=resolved_node)
        else:
            pairs = retrieve_scored(vectorstore, question, k=DEFAULT_K, node=resolved_node)

        # 闸门 2：空结果或最佳距离超阈值 → 判定无命中
        if not pairs:
            reason = "empty"
        elif pairs[0][1] > threshold:
            reason = "threshold"
        else:
            reason = "ok"

    no_match = reason != "ok"
    docs = [] if no_match else [doc for doc, _ in pairs]

    context = "\n\n---\n\n".join(d.page_content for d in docs)
    prompt_context = context or "（无检索结果）"

    # 组装完整 Prompt（在线/离线都返回，便于审计实际喂给模型的内容）
    prompt_text = prompt.format(context=prompt_context, question=question)

    base = {
        "question": question,
        "node": resolved_node,
        "no_match": no_match,
        "reason": reason,
        "threshold": threshold,
        "match_score": pairs[0][1] if pairs else None,
        "context": context,
        "prompt": prompt_text,
        "source_documents": docs,
    }

    if no_match:
        # 直接返回固定话术：不调用 LLM，杜绝其在无证据时自由发挥
        return {**base, "answer": NO_MATCH_ANSWER, "offline": False,
                "short_circuited": True}

    llm = None if force_offline else get_llm()
    if llm is None:
        # 离线兜底：不调用模型，给出检索证据和 Prompt，系统始终可运行
        return {**base, "answer": None, "offline": True, "short_circuited": False}

    # 在线 LCEL 链：Prompt → DeepSeek → 字符串解析
    chain = prompt | llm | StrOutputParser()
    result = chain.invoke({"context": prompt_context, "question": question})
    return {**base, "answer": result, "offline": False, "short_circuited": False}


def parse_questions(argv=None):
    """
    解析命令行问题：带参数时用用户自己的问题，不带参数时跑默认演示问题。
    例：python rag_qa.py "redis 内存满了怎么排查"
    """
    argv = sys.argv if argv is None else argv
    args = [a.strip() for a in argv[1:] if a and a.strip()]
    return args if args else list(DEFAULT_QUESTIONS)


def _no_match_reason(result) -> str:
    """把无命中的结构化原因翻译成一句可读的判定依据"""
    if result["reason"] == "no_signal":
        return "问题仅提及节点名，未包含运维语义，判为与本知识库无关"
    if result["reason"] == "empty" or result["match_score"] is None:
        return f"节点 {result['node'] or '（未指定）'} 在库中无记录"
    return f"最佳距离 {result['match_score']:.3f} > 阈值 {result['threshold']:.3f}"


def main() -> None:
    """命令行入口：构建/加载向量库后逐题问答；默认问题跑完再做线上引用对齐评估。"""
    vs = build_vectorstore()
    force_offline = "--offline" in sys.argv[1:]
    query_args = [arg for arg in sys.argv[1:] if arg != "--offline"]
    questions = parse_questions([sys.argv[0], *query_args])
    custom_mode = bool(query_args) or force_offline

    for question in questions:
        print("\n" + "=" * 70)
        print("问题：", question)
        result = answer(question, vs, force_offline=force_offline)
        if result["no_match"]:
            print("[无命中]", result["answer"])
            print(f"（判定依据：{_no_match_reason(result)}）")
        elif result["offline"]:
            print("[离线模式：未配置 DEEPSEEK_API_KEY，以下为检索到的历史故障证据]")
            print(result["context"][:600])
        else:
            print(result["answer"])
        print("引用来源：", format_sources(result["source_documents"]))

    if not custom_mode:
        try:
            from ai_bot.rag.rag_evaluation import EVAL_QUESTIONS

            report = evaluate_answers_online(vs, EVAL_QUESTIONS)
            if report["ok"]:
                print("\n[在线评估] 引用对齐率 =", report["citation_hit_rate"],
                      "，无命中数 =", report["no_match_count"])
        except Exception as exc:
            print("[在线评估跳过]", exc)


if __name__ == "__main__":
    main()
