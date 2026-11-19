import os
from pathlib import Path

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv(Path(__file__).resolve().parents[3] / ".env")

password = os.getenv("NEO4J_PASSWORD")
if not password:
    raise SystemExit("请在项目 .env 或环境变量中设置 NEO4J_PASSWORD。")

neo4j_driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI") or "neo4j://127.0.0.1:7687",
    auth=(os.getenv("NEO4J_USERNAME") or "neo4j", password),
)
