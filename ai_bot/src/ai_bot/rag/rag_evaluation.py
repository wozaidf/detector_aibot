"""RAG 检索质量离线评估与调参。

不依赖 LLM：用带标签的评估问题集，量化不同检索策略与 K 值的召回质量，用数据决定
rag_qa 的默认配置。指标为 Hit Rate@K（Top-K 是否命中期望文档）与 MRR（首个命中文档的
倒数排名）；对比 similarity 与 mmr 两种策略在 K=1/2/3/5 下的表现。

用法：
    python -m ai_bot.rag.rag_evaluation
"""

import os

try:                              # 作为包模块导入时走相对导入
    from . import rag_qa as rq
except ImportError:               # 兼容「python rag_evaluation.py」脚本方式运行
    import rag_qa as rq

# 评估问题集：问题 + 期望命中的节点（可空）+ 期望故障类型（可空）
# 标签来自故障表真实条目，避免主观臆造
EVAL_QUESTIONS = [
    {"q": "docker_003 的 CPU 故障怎么排查？",          "node": "docker_003", "fault": "CPU fault"},
    {"q": "容器 CPU 占用率高、怀疑有死循环进程怎么办",  "node": None,         "fault": "CPU fault"},
    {"q": "数据库连接数达到上限怎么处理",              "node": None,         "fault": "db connection limit"},
    {"q": "数据库实例关闭、突然连不上怎么办",          "node": None,         "fault": "db  close"},
    {"q": "容器网络延迟高怎么排查",                    "node": None,         "fault": "network delay"},
    {"q": "网络出现丢包怎么定位",                      "node": None,         "fault": "network loss"},
    {"q": "主机网卡发送队列堆积导致网络延迟",          "node": None,         "fault": "network delay"},
    {"q": "docker_003 这个节点历史上发生过什么故障",   "node": "docker_003", "fault": None},
    {"q": "docker_008 出了什么问题",                   "node": "docker_008", "fault": None},
    {"q": "数据库会话连接数告警如何处理",              "node": None,         "fault": "db connection limit"},
]


def _match(doc, expect_node, expect_fault):
    """判断文档是否满足期望标签（node/fault 为空表示该维度不限制）"""
    if expect_node and doc.metadata["node"] != expect_node:
        return False
    if expect_fault and doc.metadata["fault_type"] != expect_fault:
        return False
    return True


def _search(vs, question, strategy, k):
    """按策略检索，返回文档列表（按相关度排序）"""
    if strategy == "mmr":
        return vs.max_marginal_relevance_search(
            question, k=k, fetch_k=max(k * 3, 10))
    return vs.similarity_search(question, k=k)


def evaluate_one(vs, strategy, k):
    """
    评估单个（策略, K）组合，返回：
      hit_rate：命中问题占比
      mrr     ：平均倒数排名
      misses  ：未命中的问题序号列表（便于针对性分析）
    """
    hit_sum, mrr_sum, misses = 0, 0.0, []

    for i, item in enumerate(EVAL_QUESTIONS):
        docs = _search(vs, item["q"], strategy, k)
        hit_rank = 0
        for rank, doc in enumerate(docs, start=1):
            if _match(doc, item["node"], item["fault"]):
                hit_rank = rank
                break
        if hit_rank > 0:
            hit_sum += 1
            mrr_sum += 1.0 / hit_rank
        else:
            misses.append(i)

    n = len(EVAL_QUESTIONS)
    return {
        "strategy": strategy,
        "k": k,
        "hit_rate": hit_sum / n,
        "mrr": mrr_sum / n,
        "misses": misses,
    }


def run_grid(vs):
    """跑完整对比网格并打印结果表，返回全部结果"""
    results = []
    for strategy in ("similarity", "mmr"):
        for k in (1, 2, 3, 5):
            results.append(evaluate_one(vs, strategy, k))

    print(f"共 {len(EVAL_QUESTIONS)} 个评估问题")
    print(f"{'策略':<11}{'K':>2}  {'HitRate':>8}  {'MRR':>7}  未命中问题序号")
    print("-" * 62)
    for r in results:
        miss_txt = ",".join(str(x) for x in r["misses"]) or "-"
        print(f"{r['strategy']:<11}{r['k']:>2}  {r['hit_rate']:>8.2f}  "
              f"{r['mrr']:>7.3f}  {miss_txt}")
    return results


def best_config(results):
    """按 MRR 优先、HitRate 次之、K 更小更省 token 的顺序选最优配置"""
    return sorted(results,
                  key=lambda r: (-r["mrr"], -r["hit_rate"], r["k"]))[0]


if __name__ == "__main__":
    # 已有持久化库则直接复用；没有（如刚 clone）则先按语料构建，避免空库评估出全 0
    vs = rq.build_vectorstore(rq.CORPUS_DIR, rq.CHROMA_DIR)
    results = run_grid(vs)
    best = best_config(results)
    print("\n推荐配置：",
          f"strategy={best['strategy']}, k={best['k']}, "
          f"hit_rate={best['hit_rate']:.2f}, mrr={best['mrr']:.3f}")
