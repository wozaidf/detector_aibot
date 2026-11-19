"""复用项目的 Neo4j 驱动执行参数化 Cypher 查询。"""

from collections.abc import Mapping
from typing import Any

from neo4j import RoutingControl


def query_neo4j(
    cypher: str,
    parameters: Mapping[str, Any] | None = None,
    *,
    database: str = "knowledge",
) -> list[dict[str, Any]]:
    """通过读路由执行 Cypher，返回以结果列名为键的字典列表。

    动态值通过 parameters 绑定，例如 Cypher 中的 $name。
    查询直接返回节点、关系或路径时，保留 Neo4j 驱动的原生对象。
    Neo4j 的连接和查询异常会传给调用方。
    """
    from ai_bot.knowledge.neo4j_driver import neo4j_driver

    result = neo4j_driver.execute_query(
        cypher,
        parameters_=dict(parameters) if parameters is not None else None,
        database_=database,
        routing_=RoutingControl.READ,
    )
    return [dict(record) for record in result.records]


def get_kg_context(
    component_ids: list[str],
    dataset: str | None = None,
) -> list[dict[str, Any]]:
    """按组件 ID 查询指标和历史调用模式，供检索规划器使用。

    每个数据集中的组件 ID 返回一条记录，最多包含 20 条指标来源和
    10 条调用模式。未命中时返回空列表，Neo4j 异常由调用方处理。
    """
    if not component_ids:
        return []

    return query_neo4j(
        """
        MATCH (c:Component)
        WHERE c.component_id IN $component_ids
          AND ($dataset IS NULL OR c.dataset = $dataset)
        WITH c.component_id AS component_id, c.dataset AS dataset,
             min(c.name) AS component_name, collect(c) AS components

        CALL (components) {
            UNWIND components AS c
            MATCH (c)-[:HAS_METRIC_WINDOW]->(w:MetricWindow)-[:MEASURES]->(m:Metric)
            WHERE m.name IS NOT NULL
            WITH DISTINCT m.name AS name, w.source_file AS source_file
            ORDER BY name, source_file
            LIMIT 20
            RETURN collect({name: name, source_file: source_file}) AS metrics
        }

        CALL (components) {
            UNWIND components AS c
            MATCH (c)-[:HAS_CALL_PATTERN]->(p:CallPattern)
            WITH DISTINCT p{
                .pattern_id, .service_name, .data_source_name, .call_type,
                .start_time, .end_time, .span_count, .failure_count, .source_file
            } AS pattern
            ORDER BY pattern.start_time IS NULL, pattern.start_time DESC,
                     pattern.pattern_id, pattern.source_file
            LIMIT 10
            RETURN collect(pattern) AS call_patterns
        }

        RETURN component_id, component_name, dataset, metrics, call_patterns
        ORDER BY component_id, dataset
        """,
        {"component_ids": component_ids, "dataset": dataset},
    )
