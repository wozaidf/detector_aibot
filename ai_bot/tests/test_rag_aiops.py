# -*- coding: utf-8 -*-
"""
aiops_rag 的行为测试（标准库 unittest，无需额外安装）。
运行方式：python -m unittest test_aiops_rag.py -v
"""
import os
import tempfile
import unittest

from ai_bot.rag import aiops_rag as ar

# 路径跟随代码位置与 AIOPS_DATA_ROOT，不写死机器路径（便于他人 clone 后直接运行测试）
DATA_ROOT = ar.DEFAULT_DATA_ROOT
FAULT_CSV = os.path.join(DATA_ROOT, "故障整理（预赛）.csv")
# 测试产出的语料写到临时目录，避免污染仓库内纳入版本管理的 corpus/
OUT_ROOT = tempfile.mkdtemp(prefix="rag_corpus_test_")

# 语料生成依赖外部原始数据集（故障表 + 指标 zip），仓库内不含这些数据；
# 缺少数据时整个模块跳过，不影响检索/问答测试在干净环境里运行。
_REQUIRED_DATA = [
    FAULT_CSV,
    os.path.join(DATA_ROOT, "2020_04_11", "平台指标", "dcos_docker.csv"),
]


def setUpModule():
    missing = [p for p in _REQUIRED_DATA if not os.path.exists(p)]
    if missing:
        raise unittest.SkipTest(
            "需要 AIOPS 原始数据集（可用环境变量 AIOPS_DATA_ROOT 指定）："
            + "、".join(os.path.basename(p) for p in missing))


def tearDownModule():
    import shutil
    shutil.rmtree(OUT_ROOT, ignore_errors=True)


def _all_fault_rows():
    """读取故障表全部行（不按日期筛选）"""
    return ar._read_csv_with_encodings(FAULT_CSV).to_dict(orient="records")


class TestQueryMetrics(unittest.TestCase):
    """步骤 4：Agent 工具函数 query_metrics"""

    def test_docker_cpu_from_extracted_dir(self):
        res = ar.query_metrics("docker_003", "container_cpu_used",
                               "2020/4/11 0:05", "2020_04_11", data_root=DATA_ROOT)
        self.assertTrue(res["ok"], res)
        self.assertEqual(res["value"], 12.0)
        self.assertLessEqual(res["delta_sec"], 300)
        self.assertEqual(res["source_type"], "dir")

    def test_docker_cpu_from_zip_directly(self):
        res = ar.query_metrics("docker_003", "container_cpu_used",
                               "2020/4/11 0:05", "2020_04_11",
                               data_root=DATA_ROOT, prefer_zip=True)
        self.assertTrue(res["ok"], res)
        self.assertEqual(res["value"], 12.0)
        self.assertEqual(res["source_type"], "zip")

    def test_db_semicolon_kpi(self):
        res = ar.query_metrics("db_007", "Sess_Connect",
                               "2020/4/11 2:15", "2020_04_11", data_root=DATA_ROOT)
        self.assertTrue(res["ok"], res)
        self.assertIsInstance(res["value"], float)

    def test_missing_node_returns_error_not_raise(self):
        res = ar.query_metrics("docker_999", "container_cpu_used",
                               "2020/4/11 0:05", "2020_04_11", data_root=DATA_ROOT)
        self.assertFalse(res["ok"])
        self.assertIn("error", res)

    def test_missing_file_returns_error_not_raise(self):
        res = ar.query_metrics("docker_003", "container_cpu_used",
                               "2020/4/11 0:05", "2099_01_01", data_root=DATA_ROOT)
        self.assertFalse(res["ok"])


class TestSnapshotFallback(unittest.TestCase):
    """kpi 为空（network delay/loss）时的全指标快照兜底"""

    def test_node_snapshot_has_multiple_kpis(self):
        snap = ar.get_node_snapshot("docker_002", "2020/4/11 0:35",
                                    "2020_04_11", data_root=DATA_ROOT)
        self.assertTrue(snap["ok"], snap)
        self.assertGreaterEqual(len(snap["metrics"]), 5)
        self.assertIn("container_cpu_used", snap["metrics"])

    def test_empty_kpi_end_to_end_no_literal_nan(self):
        """回归：空单元格不得渲染成 'nan'，且必须走快照兜底"""
        faults = ar.load_faults(FAULT_CSV, "2020_04_11")
        row = faults[1]
        extracted = ar.extract_fault_metrics(row, data_root=DATA_ROOT)
        self.assertEqual(extracted["mode"], "snapshot")
        md = ar.render_markdown(row, extracted)
        self.assertNotIn("nan", md)
        self.assertIn("全指标快照", md)


class TestDateMapping(unittest.TestCase):
    """log_time / start_time 双时间口径的映射"""

    def test_effective_time_prefers_start_time(self):
        rows = _all_fault_rows()
        row100 = next(r for r in rows if r["index"] == 100)
        # start_time 非空时，有效定位时间取 start_time，数据日期为 05_22
        self.assertEqual(ar.effective_time(row100), "2020/5/22 0:07")
        self.assertEqual(ar.data_date_of(row100), "2020_05_22")
        # start_time 为空时回退 log_time（index 1）
        row1 = next(r for r in rows if r["index"] == 1)
        self.assertEqual(ar.effective_time(row1), "2020/4/11 0:05")
        self.assertEqual(ar.data_date_of(row1), "2020_04_11")

    def test_load_faults_by_data_date(self):
        self.assertEqual(len(ar.load_faults(FAULT_CSV, "2020_04_11")), 11)
        self.assertEqual(len(ar.load_faults(FAULT_CSV, "2020_05_22")), 7)
        self.assertEqual(len(ar.load_faults(FAULT_CSV, "2020_05_23")), 9)

    def test_data_date_of_blank_time_returns_empty(self):
        """回归：log_time 与 start_time 都为空时，NaT 不支持 strftime，必须返回空串"""
        for row in ({"log_time": "", "start_time": ""},
                    {"log_time": None, "start_time": None},
                    {"log_time": "不是时间", "start_time": ""}):
            self.assertEqual(ar.data_date_of(row), "")

    def test_load_faults_skips_blank_time_rows(self):
        """回归：故障表出现空时间行时，批量筛选不得整体崩溃"""
        import tempfile
        header = ("index,object,fault_desrcibtion,kpi,name,container,log_time,"
                  "log_block,block,start_time,duration\n")
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "faults.csv")
            with open(path, "w", encoding="utf-8-sig", newline="") as f:
                f.write(header)
                f.write("900,docker,CPU fault,container_cpu_used,docker_003,c1,"
                        "2020/4/11 0:05,,,,\n")
                f.write("901,docker,CPU fault,container_cpu_used,docker_003,c1,,,,,\n")   # 两个时间列都空
            rows = ar.load_faults(path, "2020_04_11")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["index"], 900)


class TestMarkdownAndCorpus(unittest.TestCase):
    """RAG 语料渲染与批量生成"""

    def test_render_markdown_contains_key_fields(self):
        faults = ar.load_faults(FAULT_CSV, "2020_04_11")
        md = ar.render_markdown(faults[0], ar.extract_fault_metrics(faults[0], data_root=DATA_ROOT))
        self.assertIn("## 历史故障报告", md)
        self.assertIn("docker_003", md)
        self.assertIn("12.0", md)

    def test_render_shows_locate_time_when_mapped(self):
        """start_time 与 log_time 不同时，语料需同时展示两个时间口径"""
        row = next(r for r in _all_fault_rows() if r["index"] == 100)
        md = ar.render_markdown(row, ar.extract_fault_metrics(row, data_root=DATA_ROOT))
        self.assertIn("发生时间", md)
        self.assertIn("数据定位时间", md)
        self.assertIn("2020/5/15 0:07", md)
        self.assertIn("2020/5/22 0:07", md)

    def test_build_corpus_one_day_11_files(self):
        paths = ar.build_corpus("2020_04_11", fault_csv=FAULT_CSV,
                                out_root=OUT_ROOT, data_root=DATA_ROOT)
        self.assertEqual(len(paths), 11)
        self.assertTrue(all(os.path.exists(p) for p in paths))
        self.assertTrue(os.path.exists(os.path.join(OUT_ROOT, "2020_04_11",
                                                    "all_incidents_2020_04_11.md")))

    def test_build_all_corpus_covers_27_faults(self):
        """当前 7 个 zip 中 3 天有故障标注，共 11+7+9=27 条"""
        report = ar.build_all_corpus(fault_csv=FAULT_CSV,
                                     out_root=OUT_ROOT, data_root=DATA_ROOT)
        total = sum(len(v) for v in report.values())
        self.assertEqual(total, 27)
        self.assertEqual(set(report.keys()),
                         {"2020_04_11", "2020_05_22", "2020_05_23"})
        self.assertTrue(os.path.exists(os.path.join(OUT_ROOT, "all_incidents.md")))


class TestBaselineCorpus(unittest.TestCase):
    """正常基线语料：把无故障标注的日期（04_20~04_23）转成可检索的基线知识"""

    @classmethod
    def setUpClass(cls):
        import tempfile
        cls.tmpdir = tempfile.mkdtemp(prefix="baseline_test_")
        cls.paths = ar.build_baseline_corpus(
            dates=["2020_04_20"], out_dir=cls.tmpdir,
            data_root=DATA_ROOT, prefer_zip=True)

    @classmethod
    def tearDownClass(cls):
        import shutil
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def test_one_node_one_document(self):
        # 单日 57 个节点 -> 57 份基线文档（不是每指标一份，避免语料碎片化）
        self.assertEqual(len(self.paths), 57)

    def test_markdown_has_baseline_structure(self):
        sample = self.paths[0]
        with open(sample, "r", encoding="utf-8") as f:
            md = f.read()
        self.assertIn("## 正常基线", md)
        self.assertIn("中位数", md)
        self.assertIn("P90", md)
        self.assertIn("正常区间", md)
        # 文档里必须能定位到节点名
        self.assertRegex(md, r"## 正常基线：\w+_\d+")

    def test_multiple_dates_aggregated_into_same_node_doc(self):
        # 同一节点在 2 天里仍只产出一份文档（跨日聚合，避免近重复稀释检索）
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            paths = ar.build_baseline_corpus(
                dates=["2020_04_20", "2020_04_21"], out_dir=tmp,
                data_root=DATA_ROOT, prefer_zip=True)
            self.assertEqual(len(paths), 57)

    def test_selected_dates_exclude_fault_dates(self):
        # 不显式传 dates 时，只挑「故障表中无标注」的日期
        dates = ar.baseline_dates(FAULT_CSV, DATA_ROOT)
        self.assertIn("2020_04_20", dates)
        self.assertIn("2020_04_23", dates)
        self.assertNotIn("2020_04_11", dates)   # 有故障标注，不进基线
        self.assertNotIn("2020_05_22", dates)


if __name__ == "__main__":
    unittest.main(verbosity=2)
