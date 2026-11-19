"""将已构造成 L4 输出格式、但未通过发布阈值的事件保存到 PostgreSQL。"""

import hashlib
import json

import psycopg


class RejectedEventStore:
    """L4 阈值未通过事件存储；以来源 Topic 和原事件 ID 幂等 upsert。"""

    def __init__(self, dsn: str):
        self.connection = psycopg.connect(dsn, autocommit=True)
        with self.connection.cursor() as cursor:
            # 创建表格，包含来源 Topic、原事件 ID、实体 ID、阶段、诊断分数、阈值、拒绝原因、事件 JSON 负载，以及首次和最后拒绝时间戳。
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS l4_rejected_events (
                  source_topic TEXT NOT NULL,
                  source_event_id TEXT NOT NULL,
                  entity_id TEXT,
                  stage TEXT,
                  diagnosis_score DOUBLE PRECISION NOT NULL,
                  diagnosis_threshold DOUBLE PRECISION NOT NULL,
                  rejection_reason TEXT NOT NULL,
                  event_payload JSONB NOT NULL,
                  first_rejected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  last_rejected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  PRIMARY KEY (source_topic, source_event_id)
                )
                """)
            # 校验主键约束。
            cursor.execute("""
                DO $$
                DECLARE current_pk TEXT;
                DECLARE current_columns TEXT[];
                BEGIN
                  SELECT c.conname, array_agg(a.attname ORDER BY key_column.ordinality)
                    INTO current_pk, current_columns
                  FROM pg_constraint c
                  JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS key_column(attnum, ordinality) ON TRUE
                  JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = key_column.attnum
                  WHERE c.conrelid = 'l4_rejected_events'::regclass AND c.contype = 'p'
                  GROUP BY c.conname;
                  IF current_columns IS DISTINCT FROM ARRAY['source_topic', 'source_event_id']::TEXT[] THEN
                    IF current_pk IS NOT NULL THEN
                      EXECUTE format('ALTER TABLE l4_rejected_events DROP CONSTRAINT %I', current_pk);
                    END IF;
                  END IF;
                  ALTER TABLE l4_rejected_events DROP COLUMN IF EXISTS record_format;
                  SELECT c.conname, array_agg(a.attname ORDER BY key_column.ordinality)
                    INTO current_pk, current_columns
                  FROM pg_constraint c
                  JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS key_column(attnum, ordinality) ON TRUE
                  JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = key_column.attnum
                  WHERE c.conrelid = 'l4_rejected_events'::regclass AND c.contype = 'p'
                  GROUP BY c.conname;
                  IF current_pk IS NULL THEN
                    ALTER TABLE l4_rejected_events
                      ADD CONSTRAINT l4_rejected_events_pkey PRIMARY KEY (source_topic, source_event_id);
                  END IF;
                END $$
                """)
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_l4_rejected_entity_time ON l4_rejected_events (entity_id, last_rejected_at DESC)")

    def store(self, source_topic: str, event: dict, score: float, threshold: float,
              source_event: dict | None = None) -> None:
        """保存 L4 输出事件 JSON。"""
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not event_id.strip():
            id_source = source_event if source_event is not None else event
            canonical = json.dumps(id_source, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            event_id = "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        print(f"event数据格式{type(event)}") # <class 'dict'>
        payload = json.dumps(event, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        with self.connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO l4_rejected_events
                  (source_topic, source_event_id, entity_id, stage, diagnosis_score,
                   diagnosis_threshold, rejection_reason, event_payload)
                VALUES (%s, %s, %s, %s, %s, %s, 'BELOW_DIAGNOSIS_THRESHOLD', %s::jsonb)
                ON CONFLICT (source_topic, source_event_id) DO UPDATE SET
                  entity_id=EXCLUDED.entity_id, stage=EXCLUDED.stage,
                  diagnosis_score=EXCLUDED.diagnosis_score,
                  diagnosis_threshold=EXCLUDED.diagnosis_threshold,
                  rejection_reason=EXCLUDED.rejection_reason,
                  event_payload=EXCLUDED.event_payload, last_rejected_at=now()
                """, (source_topic, event_id, event.get("entity_id"), event.get("stage"), score, threshold, payload))

    def close(self) -> None:
        self.connection.close()
