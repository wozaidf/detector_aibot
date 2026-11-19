"""Graph schema for the 2018 AIOps metrics and trace datasets."""

NODE_TYPES = [
    {
        "label": "Component",
        "description": "A source-identified service, host, database, container, or runtime component.",
        "additional_properties": False,
        "properties": [
            {"name": "dataset", "type": "STRING"},
            {"name": "component_uid", "type": "STRING"},
            {"name": "component_id", "type": "STRING"},
            {"name": "component_type", "type": "STRING"},
            {"name": "name", "type": "STRING"},
        ],
    },
    {
        "label": "Metric",
        "description": "A metric name observed for a component.",
        "additional_properties": False,
        "properties": [
            {"name": "dataset", "type": "STRING"},
            {"name": "metric_id", "type": "STRING"},
            {"name": "name", "type": "STRING"},
            {"name": "category", "type": "STRING"},
        ],
    },
    {
        "label": "MetricWindow",
        "description": "Time-bucketed metric statistics aggregated from raw samples.",
        "additional_properties": False,
        "properties": [
            {"name": "dataset", "type": "STRING"},
            {"name": "window_id", "type": "STRING"},
            {"name": "start_time", "type": "STRING"},
            {"name": "end_time", "type": "STRING"},
            {"name": "sample_count", "type": "INTEGER"},
            {"name": "value_min", "type": "FLOAT"},
            {"name": "value_max", "type": "FLOAT"},
            {"name": "value_avg", "type": "FLOAT"},
            {"name": "source_file", "type": "STRING"},
        ],
    },
    {
        "label": "CallPattern",
        "description": "A time-bucketed summary of observed trace spans.",
        "additional_properties": False,
        "properties": [
            {"name": "dataset", "type": "STRING"},
            {"name": "pattern_id", "type": "STRING"},
            {"name": "call_type", "type": "STRING"},
            {"name": "service_name", "type": "STRING"},
            {"name": "data_source_name", "type": "STRING"},
            {"name": "start_time", "type": "STRING"},
            {"name": "end_time", "type": "STRING"},
            {"name": "span_count", "type": "INTEGER"},
            {"name": "success_count", "type": "INTEGER"},
            {"name": "failure_count", "type": "INTEGER"},
            {"name": "elapsed_avg_ms", "type": "FLOAT"},
            {"name": "elapsed_max_ms", "type": "FLOAT"},
            {"name": "source_file", "type": "STRING"},
        ],
    },
]

RELATIONSHIP_TYPES = [
    {"label": "HAS_METRIC_WINDOW", "description": "A component has observations in a metric window."},
    {"label": "MEASURES", "description": "A metric window summarizes a named metric."},
    {"label": "HAS_CALL_PATTERN", "description": "A component produced the summarized trace spans."},
]

PATTERNS = [
    ("Component", "HAS_METRIC_WINDOW", "MetricWindow"),
    ("MetricWindow", "MEASURES", "Metric"),
    ("Component", "HAS_CALL_PATTERN", "CallPattern"),
]

SCHEMA = {
    "node_types": NODE_TYPES,
    "relationship_types": RELATIONSHIP_TYPES,
    "patterns": PATTERNS,
    "additional_node_types": False,
    "additional_relationship_types": False,
    "additional_patterns": False,
}
