"""AIOps Challenge 2020 历史故障知识库构建工具。

流程：
    1. 读取故障表（故障整理（预赛）.csv），按数据日期筛选故障记录；
    2. 定位对应日期的指标数据（支持已解压目录与 zip 直读两种模式）；
    3. 按节点 + 指标名 + 故障时间点提取真实指标值，kpi 为空时用节点全指标快照兜底；
    4. 渲染成 RAG 所需的 Markdown 语料，写入 corpus/。

指标源为长表结构（itemid/name/bomc_id/timestamp/value/cmdb_id）；读取链路带编码容错
（utf-8/utf-8-sig/gbk/gb18030），大文件分块以免一次性载入内存。所有找不到文件、节点或
指标的情况都返回结构化错误，不抛异常打断批处理。
"""

import io
import os
import zipfile
from datetime import datetime, timezone, timedelta

import pandas as pd

# 全局常量
# 数据默认根目录（原始故障表 + 指标 zip 所在目录）。
# 可用环境变量 AIOPS_DATA_ROOT 覆盖，便于他人 clone 后指向自己的数据路径；
# 各函数的 data_root 参数仍可单独覆盖，优先级最高。
DEFAULT_DATA_ROOT = os.environ.get("AIOPS_DATA_ROOT", r"E:\company\data")

# 本项目目录（语料输出默认落在这里的 corpus/ 下，随代码位置走，不依赖 data_root）
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
CORPUS_DIRNAME = "corpus"

# 数据集时间戳为北京时间（UTC+8）的毫秒时间戳，固定时区避免依赖机器本地设置
TZ_BJ = timezone(timedelta(hours=8))

# 节点前缀 -> 指标文件路由（子目录名, 文件名）
# 节点名形如 docker_003 / db_007 / os_018 / container_001，下划线首段即类型
NODE_ROUTES = {
    "docker":    ("平台指标", "dcos_docker.csv"),
    "container": ("平台指标", "dcos_container.csv"),
    "db":        ("平台指标", "db_oracle_11g.csv"),
    "os":        ("平台指标", "os_linux.csv"),
    "redis":     ("平台指标", "mw_redis.csv"),
}

# 读取指标 CSV 时只保留这 4 列，降低内存占用（itemid/bomc_id 分析用不到）
USE_COLS = ["name", "timestamp", "value", "cmdb_id"]

# 超过该大小（字节）的文件启用分块读取（调用链指标可达数百 MB）
LARGE_FILE_THRESHOLD = 50 * 1024 * 1024
CHUNK_SIZE = 200_000

# 编码尝试顺序：utf-8-sig 可兼容带 BOM 的 UTF-8
ENCODING_CANDIDATES = ("utf-8-sig", "utf-8", "gbk", "gb18030")

# 故障排查建议规则库（确定性规则，替代在线 LLM；可按团队经验持续扩充）
REPAIR_ADVICE = {
    ("docker", "CPU fault"):
        "通过 docker stats/top 定位容器内高 CPU 进程，排查死循环、频繁 Full GC 或计算热点，"
        "必要时做 CPU limit 限流、水平扩容或重启异常实例。",
    ("docker", "network delay"):
        "检查容器网络延迟与上下游调用链，排查宿主机网络拥塞、DNS 解析慢、连接池耗尽及跨机房调用，"
        "结合 tcpdump/容器网络指标定位瓶颈链路。",
    ("docker", "network loss"):
        "检查容器虚拟网卡与宿主机丢包率，排查 conntrack 表满、UDP 缓冲区不足、物理链路抖动，"
        "并核对是否存在邻居节点过载或网络策略变更。",
    ("db", "db connection limit"):
        "检查数据库会话数与进程连接数是否逼近上限，排查连接泄漏、长事务和未释放连接池，"
        "评估 processes/sessions 参数并在应用侧收敛连接池配置。",
    ("db", "db  close"):
        "检查数据库实例存活状态与监听服务，核查 tnsping 连通性、告警日志与主机资源，"
        "确认是否实例宕机或监听异常，并按预案执行重启或主备切换。",
    ("os", "network delay"):
        "检查主机网卡发送/接收队列堆积情况，排查带宽打满、路由抖动与对端 ACK 延迟，"
        "结合 netstat/ss 观察重传率与队列积压。",
    ("os", "network loss"):
        "检查主机网卡丢包与错误包计数，排查物理链路、交换机端口、内核网络缓冲区及防火墙策略，"
        "必要时切换网卡或链路。",
}
GENERIC_ADVICE = "结合故障时间点前后的指标与调用链做关联分析，先确认影响范围，再按变更/资源/依赖三条线排查。"


def parse_timestamp_ms(timestamp):
    """
    把多种形式的时间统一转换为「北京时间的毫秒时间戳」。
    入参支持：
      - 字符串：'2020/4/11 0:05'、'2020-04-11 00:05:00'
      - datetime / pandas.Timestamp（naive 视为北京时间）
      - int/float：直接当作毫秒时间戳
    """
    if isinstance(timestamp, (int, float)):
        return int(timestamp)

    ts = pd.to_datetime(timestamp)  # 自动识别常见字符串格式，得到 naive 时间
    if isinstance(ts, pd.Timestamp):
        dt = ts.to_pydatetime()
    else:
        dt = ts
    # naive 时间固定按 UTC+8 解释，再取毫秒时间戳
    return int(dt.replace(tzinfo=TZ_BJ).timestamp() * 1000)


def ms_to_bj_str(ms):
    """毫秒时间戳 -> 北京时间字符串 'YYYY-MM-DD HH:MM:SS'"""
    return datetime.fromtimestamp(ms / 1000, tz=TZ_BJ).strftime("%Y-%m-%d %H:%M:%S")


def _route_for_node(node_name):
    """根据节点名前缀返回 (子目录, 文件名)，无法识别时返回 None"""
    prefix = str(node_name).split("_", 1)[0]
    return NODE_ROUTES.get(prefix)


def _open_metric_source(date_str, subdir, fname, data_root, prefer_zip):
    """
    定位指标文件，返回 (source, source_type, source_label)：
      - 目录模式：source 为本地文件路径（str），source_type='dir'
      - zip 模式：source 为 BytesIO（zip 内目标条目已读入内存），source_type='zip'
    定位优先级：prefer_zip=False 时先目录后 zip；prefer_zip=True 时强制 zip。
    找不到任何数据源时返回 (None, None, None)。
    """
    dir_path = os.path.join(data_root, date_str, subdir, fname)
    zip_path = os.path.join(data_root, f"{date_str}.zip")

    # 1) 目录模式
    if not prefer_zip and os.path.exists(dir_path):
        return dir_path, "dir", dir_path

    # 2) zip 模式：打开压缩包并在条目列表中模糊匹配（兼容目录名编码差异）
    if os.path.exists(zip_path):
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                target_entry = None
                for entry in zf.namelist():
                    # 用正斜杠统一比较，要求条目以 /子目录/文件名 结尾
                    norm = entry.replace("\\", "/")
                    if norm.endswith(f"/{subdir}/{fname}") or norm == f"{date_str}/{subdir}/{fname}":
                        target_entry = entry
                        break
                if target_entry is None:
                    return None, None, None
                buf = io.BytesIO(zf.read(target_entry))  # 读入内存，zip 句柄随后关闭
            return buf, "zip", f"{zip_path}!{target_entry}"
        except zipfile.BadZipFile:
            return None, None, None

    # 3) 目录兜底（prefer_zip 但 zip 不存在时）
    if os.path.exists(dir_path):
        return dir_path, "dir", dir_path
    return None, None, None


def _read_csv_with_encodings(source, **kwargs):
    """
    按候选编码顺序尝试读取 CSV，全部失败则抛出最后一个异常。
    source 为 BytesIO 时，每次重试前 seek(0) 重置游标。
    """
    last_err = None
    for enc in ENCODING_CANDIDATES:
        try:
            if hasattr(source, "seek"):
                source.seek(0)
            return pd.read_csv(source, encoding=enc, **kwargs)
        except UnicodeDecodeError as e:
            last_err = e
            continue
    raise last_err


def read_metric_df(date_str, subdir, fname, data_root, prefer_zip=False):
    """
    读取一张指标表并返回 DataFrame，包含 name/timestamp/value/cmdb_id 四列。
    大文件（仅目录模式可获知大小）走分块过滤读取，降低内存峰值。
    """
    source, source_type, source_label = _open_metric_source(
        date_str, subdir, fname, data_root, prefer_zip)
    if source is None:
        raise FileNotFoundError(f"未找到指标文件：date={date_str}, file={fname}")

    # 目录模式且文件较大：分块读取，只保留需要的列（此处无法按节点预过滤，
    # 因为调用方可能需要全节点快照；分块仍能显著降低瞬时内存）
    if source_type == "dir" and os.path.getsize(source) > LARGE_FILE_THRESHOLD:
        chunks = []
        for chunk in pd.read_csv(source, usecols=USE_COLS, chunksize=CHUNK_SIZE,
                                 low_memory=False):
            chunks.append(chunk)
        df = pd.concat(chunks, ignore_index=True)
    else:
        df = _read_csv_with_encodings(source, usecols=USE_COLS, low_memory=False)

    # 时间戳/数值强制转数值类型，脏数据变成 NaN 后由上层判断
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df.attrs["source_type"] = source_type
    df.attrs["source_label"] = source_label
    return df


def query_metrics(node_name, kpi, timestamp, date_str,
                  window_sec=300, data_root=DEFAULT_DATA_ROOT, prefer_zip=False):
    """
    查询某节点在某时间点附近的单个指标值。

    参数
    ----
    node_name : str       节点名，如 'docker_003'
    kpi       : str       指标名，如 'container_cpu_used'
    timestamp : str/datetime/int  故障时间点，如 '2020/4/11 0:05'
    date_str  : str       日期目录/压缩包名，如 '2020_04_11'
    window_sec: int       容差窗口（秒），最近采样点超出该窗口则视为未取到
    data_root : str       数据根目录
    prefer_zip: bool      True 时强制直接读 zip（模拟未解压场景）

    返回
    ----
    成功：{"ok": True, "value": float, "time_str": ..., "delta_sec": ..., ...}
    失败：{"ok": False, "error": "原因"}  —— 不抛异常，便于 Agent 批量调用
    """
    try:
        route = _route_for_node(node_name)
        if route is None:
            return {"ok": False, "node": node_name, "kpi": kpi,
                    "error": f"无法识别的节点类型：{node_name}"}
        subdir, fname = route
        target_ms = parse_timestamp_ms(timestamp)

        df = read_metric_df(date_str, subdir, fname, data_root, prefer_zip)

        # 精准过滤：节点 + 指标名
        hit = df[(df["cmdb_id"] == node_name) & (df["name"] == kpi)].copy()
        if hit.empty:
            return {"ok": False, "node": node_name, "kpi": kpi,
                    "date_str": date_str,
                    "error": f"指标文件 {fname} 中不存在节点 {node_name} 的指标 {kpi}"}

        # 按与故障时间点的绝对距离选最近采样点
        hit["delta_ms"] = (hit["timestamp"] - target_ms).abs()
        best = hit.sort_values("delta_ms").iloc[0]
        delta_sec = float(best["delta_ms"]) / 1000
        value = best["value"]

        if pd.isna(value):
            return {"ok": False, "node": node_name, "kpi": kpi,
                    "error": "最近采样点数值为空(NaN)"}
        if delta_sec > window_sec:
            return {"ok": False, "node": node_name, "kpi": kpi,
                    "error": f"最近采样点距故障时间 {delta_sec:.0f} 秒，超出容差窗口 {window_sec} 秒"}

        return {
            "ok": True,
            "node": node_name,
            "kpi": kpi,
            "value": float(value),
            "timestamp": int(best["timestamp"]),
            "time_str": ms_to_bj_str(int(best["timestamp"])),
            "delta_sec": delta_sec,
            "source_type": df.attrs.get("source_type"),
            "source_label": df.attrs.get("source_label"),
        }
    except FileNotFoundError as e:
        return {"ok": False, "node": node_name, "kpi": kpi, "error": str(e)}
    except KeyError as e:
        return {"ok": False, "node": node_name, "kpi": kpi,
                "error": f"指标文件缺少必要列：{e}"}
    except Exception as e:  # 兜底：任何意外都转成结构化错误，保证 Agent 不中断
        return {"ok": False, "node": node_name, "kpi": kpi,
                "error": f"{type(e).__name__}: {e}"}


def get_node_snapshot(node_name, timestamp, date_str,
                      window_sec=300, data_root=DEFAULT_DATA_ROOT, prefer_zip=False):
    """
    节点全指标快照（kpi 为空时的兜底策略）：
    读取该节点在故障时间点「每一种指标」的最近采样值，返回扁平字典。
    """
    try:
        route = _route_for_node(node_name)
        if route is None:
            return {"ok": False, "node": node_name, "error": f"无法识别的节点类型：{node_name}"}
        subdir, fname = route
        target_ms = parse_timestamp_ms(timestamp)

        df = read_metric_df(date_str, subdir, fname, data_root, prefer_zip)
        hit = df[df["cmdb_id"] == node_name].copy()
        if hit.empty:
            return {"ok": False, "node": node_name,
                    "error": f"文件 {fname} 中不存在节点 {node_name}"}

        metrics, details = {}, {}
        for kpi_name, grp in hit.groupby("name"):
            grp = grp.assign(delta_ms=(grp["timestamp"] - target_ms).abs())
            best = grp.sort_values("delta_ms").iloc[0]
            if pd.isna(best["value"]) or best["delta_ms"] / 1000 > window_sec:
                continue
            metrics[kpi_name] = float(best["value"])
            details[kpi_name] = {
                "time_str": ms_to_bj_str(int(best["timestamp"])),
                "delta_sec": float(best["delta_ms"]) / 1000,
            }

        if not metrics:
            return {"ok": False, "node": node_name, "error": "容差窗口内无任何有效采样点"}
        return {
            "ok": True,
            "node": node_name,
            "target_time_str": ms_to_bj_str(target_ms),
            "metrics": metrics,
            "details": details,
            "source_type": df.attrs.get("source_type"),
        }
    except Exception as e:
        return {"ok": False, "node": node_name, "error": f"{type(e).__name__}: {e}"}


def effective_time(row):
    """
    返回指标定位用的「有效时间」字符串：
    start_time 非空时取 start_time（比赛方对后期故障做了日期平移），
    start_time 为空时回退到 log_time（4/11 第一批故障）。
    """
    start = _clean(row.get("start_time"))
    return start if start else _clean(row.get("log_time"))


def data_date_of(row):
    """
    有效时间对应的数据日期（zip/目录名），形如 '2020_05_22'。

    边界处理：log_time / start_time 都为空或格式非法时返回空串，
    而不是让 NaT 走到 strftime 抛 ValueError —— 否则故障表里只要有一行缺时间，
    整个 build_all_corpus 的批量筛选就会中断。
    """
    t = effective_time(row)
    if not t:
        return ""
    ts = pd.to_datetime(t, errors="coerce")   # 非法格式转成 NaT，不抛异常
    if pd.isna(ts):
        return ""
    return ts.strftime("%Y_%m_%d")


def load_faults(fault_csv, date_str):
    """
    读取故障表并按「数据日期」筛选（start_time 优先，空则用 log_time）。
    用解析后的日期比较，避免 '2020/4/11' 无前导零的字符串匹配问题。
    """
    df = _read_csv_with_encodings(fault_csv)  # 同样走编码容错
    day_tag = df.apply(data_date_of, axis=1)
    sub = df[day_tag == date_str].sort_values("index")
    return sub.to_dict(orient="records") if not sub.empty else []


def extract_fault_metrics(row, data_root=DEFAULT_DATA_ROOT, prefer_zip=False):
    """
    针对一条故障记录提取指标：
      - kpi 字段非空：可能是分号分隔的多个指标，逐个 query_metrics；
      - kpi 字段为空：走节点全指标快照兜底。
    """
    node = row["name"]
    eff_time = effective_time(row)          # 指标按有效时间定位
    date_str = data_date_of(row)
    kpi_raw = _clean(row.get("kpi"))

    if kpi_raw:
        results = {}
        for kpi_name in [k.strip() for k in kpi_raw.split(";") if k.strip()]:
            results[kpi_name] = query_metrics(
                node, kpi_name, eff_time, date_str,
                data_root=data_root, prefer_zip=prefer_zip)
        return {"mode": "kpi", "results": results}

    snapshot = get_node_snapshot(
        node, eff_time, date_str, data_root=data_root, prefer_zip=prefer_zip)
    return {"mode": "snapshot", "snapshot": snapshot}


def build_advice(obj, fault_desc):
    """按 (对象, 故障类型) 取排查建议，未命中规则时返回通用建议"""
    return REPAIR_ADVICE.get((obj, fault_desc), GENERIC_ADVICE)


def _clean(v):
    """
    空值清洗：pandas 把空单元格读成 float NaN，而 NaN 是 truthy，
    直接 str(NaN) 会得到字面量 'nan'。统一把 None/NaN/空串/字符串'nan' 归一成空串。
    """
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    s = str(v).strip()
    return "" if s.lower() == "nan" else s


def _fmt_num(v):
    """数值格式化：整数浮点去掉多余小数位，保留 4 位有效小数上限"""
    if v is None:
        return "N/A"
    if float(v).is_integer():
        return str(float(v))
    return f"{v:.4f}".rstrip("0").rstrip(".")


def render_markdown(row, extracted):
    """
    按任务模板把一条故障 + 真实指标渲染成 Markdown 文本。
    """
    obj = row["object"]
    fault = row["fault_desrcibtion"]
    node = row["name"]
    container = _clean(row.get("container")) or "未标注"
    log_time = _clean(row.get("log_time"))
    eff_time = effective_time(row)
    kpi_raw = _clean(row.get("kpi")) or "未指定（故障表未标注具体指标）"
    duration = _clean(row.get("duration")) or "未知"

    # 仅当比赛方做了日期平移（start_time 与 log_time 不同）时，额外展示数据定位时间
    locate_line = (f"- **数据定位时间**：{eff_time}"
                   f"（原始故障经日期平移，指标按此时间点提取）\n") if eff_time != log_time else ""

    # ---- 组装「真实数值提取结果」段落 ----
    metric_lines = []
    if extracted["mode"] == "kpi":
        for kpi_name, res in extracted["results"].items():
            if res["ok"]:
                metric_lines.append(
                    f"  - {kpi_name} = **{_fmt_num(res['value'])}**"
                    f"（实际采集时间 {res['time_str']}，距故障点 {res['delta_sec']:.0f} 秒）")
            else:
                metric_lines.append(f"  - {kpi_name} = 未提取到（{res['error']}）")
        metric_block = "\n".join(metric_lines)
    else:
        snap = extracted["snapshot"]
        if snap["ok"]:
            metric_lines.append(
                "  - 故障表未指定具体 KPI，已自动提取该节点在故障时间点的全指标快照：")
            for kpi_name, val in snap["metrics"].items():
                d = snap["details"][kpi_name]
                metric_lines.append(
                    f"    - {kpi_name} = **{_fmt_num(val)}**"
                    f"（{d['time_str']}，距故障点 {d['delta_sec']:.0f} 秒）")
        else:
            metric_lines.append(f"  - 全指标快照提取失败：{snap['error']}")
        metric_block = "\n".join(metric_lines)

    md = f"""## 历史故障报告：{obj} - {fault}
- **发生时间**：{log_time}
{locate_line}- **故障节点**：{node}（所属容器：{container}）
- **异常指标**：{kpi_raw}
  真实数值提取结果：
{metric_block}
- **故障描述**：监控系统检测到 {node} 发生 {fault} 故障，核心指标 {kpi_raw} 出现异常波动。
- **影响时长**：持续约 {duration}。
- **处理建议**：{build_advice(obj, fault)}
"""
    return md


#   6.1 正常基线语料（无故障标注日期 -> 可检索的正常区间知识）
#   6.2 故障语料（故障表标注 + 真实指标）
def baseline_dates(fault_csv=None, data_root=DEFAULT_DATA_ROOT):
    """
    返回「有数据包但故障表中无标注」的日期（如 2020_04_20 ~ 2020_04_23）。
    这些日期没有故障可写，但指标是完整可用的，适合做成正常基线。
    """
    fault_csv = fault_csv or os.path.join(data_root, "故障整理（预赛）.csv")
    return [d for d in list_available_dates(data_root) if not load_faults(fault_csv, d)]


def _fmt_stat(v):
    """基线统计数值格式化：大数留 1 位小数，常规值留 2 位"""
    if v is None or pd.isna(v):
        return "N/A"
    return f"{v:.1f}" if abs(float(v)) >= 100 else f"{float(v):.2f}"


def render_baseline_markdown(node, node_type, dates, node_df):
    """
    把一个节点在多个正常时段的指标分布渲染成 Markdown。
    粒度＝一个节点一份：既能让 RAG 按节点精准命中，又避免逐指标碎片化。
    """
    # 逐指标统计：中位数代表常态，P90 代表高位，最大值代表历史峰值
    stats = node_df.groupby("name")["value"].agg(
        points="count",
        median="median",
        p90=lambda s: s.quantile(0.90),
        maxv="max",
    ).sort_index()

    span = f"{dates[0]} ~ {dates[-1]}" if len(dates) > 1 else dates[0]

    lines = [
        f"## 正常基线：{node}（{node_type}）",
        "",
        f"- **统计来源**：{span}（{len(dates)} 个正常时段，故障表中该日无故障标注）",
        f"- **采样点数**：{len(node_df)}（覆盖 {len(stats)} 种指标）",
        "- **采样口径**：每日 00:00–06:00，约 60 秒一个采样点",
        "- **用途**：判断实时指标是否偏离正常区间——中位数≈常态，P90≈高位，最大值≈历史上限",
        "- **口径说明**：本基线以「故障表中该日无故障标注」为依据；若该时段实际存在压测或高负载，"
        "正常区间会整体偏高，使用时需结合业务负载水平判断",
        "",
        "| 指标 | 中位数 | P90 | 最大值 |",
        "|---|---|---|---|",
    ]
    for name, r in stats.iterrows():
        # 指标名可能含 | 等字符，避免破坏 Markdown 表格
        safe_name = str(name).replace("|", "/")
        lines.append(
            f"| {safe_name} | {_fmt_stat(r['median'])} | "
            f"{_fmt_stat(r['p90'])} | {_fmt_stat(r['maxv'])} |"
        )

    return "\n".join(lines) + "\n"


def build_baseline_corpus(dates=None, out_dir=None,
                          data_root=DEFAULT_DATA_ROOT, prefer_zip=True):
    """
    生成正常基线语料：按节点聚合（多日合并成一份），输出到 output/baseline/。

    参数
    ----
    dates    : list   参与统计的日期；不传时自动取「故障表无标注」的日期；
    out_dir  : str    输出目录，默认 output/baseline；
    prefer_zip: bool  默认 True，直接读 zip，无需解压。

    返回每份基线的文件路径列表（一个节点一份）。
    """
    dates = dates or baseline_dates(None, data_root)
    if not dates:
        print("[WARN] 没有可用于生成正常基线的日期")
        return []

    out_dir = out_dir or os.path.join(PROJECT_DIR, CORPUS_DIRNAME, "baseline")
    os.makedirs(out_dir, exist_ok=True)

    path_list = []
    # 按节点类型逐类处理，避免 4 天 × 5 张表同时驻留内存
    for node_type, (subdir, fname) in NODE_ROUTES.items():
        frames = []
        for d in dates:
            try:
                df = read_metric_df(d, subdir, fname, data_root, prefer_zip)
            except Exception as e:
                # 单日单表读失败不影响其余数据
                print(f"[WARN] {d} 读取 {fname} 失败：{type(e).__name__}: {e}")
                continue
            if df.empty:
                continue
            df = df.copy()
            df["data_date"] = d
            frames.append(df)
        if not frames:
            continue

        big = pd.concat(frames, ignore_index=True)
        big = big.dropna(subset=["value", "cmdb_id", "name"])

        # 一个节点聚合为一份文档
        for node, g in big.groupby("cmdb_id", sort=True):
            md = render_baseline_markdown(node, node_type, dates, g)
            fpath = os.path.join(out_dir, f"{_safe_filename(str(node))}_baseline.md")
            with open(fpath, "w", encoding="utf-8") as f:
                f.write(md)
            path_list.append(fpath)

        del big, frames   # 及时释放，控制峰值内存

    span = f"{dates[0]}~{dates[-1]}" if len(dates) > 1 else dates[0]
    print(f"[OK] 正常基线：{len(dates)} 个时段（{span}）-> {len(path_list)} 份文档 -> {out_dir}")
    return path_list


def _safe_filename(text):
    """把故障描述等文本转成安全的文件名字符"""
    for ch in ' \\ / : * ? " < > |':
        text = str(text).replace(ch, "_")
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_")


def list_available_dates(data_root=DEFAULT_DATA_ROOT):
    """
    扫描数据根目录，返回所有可用数据日期（存在同名 zip 或解压目录即算可用）。
    """
    dates = set()
    for name in os.listdir(data_root):
        full = os.path.join(data_root, name)
        if name.endswith(".zip"):
            dates.add(name[:-4])                 # 2020_05_22.zip -> 2020_05_22
        elif os.path.isdir(full) and name.startswith("2020"):
            dates.add(name)
    return sorted(dates)


def _build_one_day(date_str, rows, out_root, data_root, prefer_zip):
    """
    内部函数：为某一日期的故障生成单条 md + 当日汇总，
    返回 (单条路径列表, 当日 md 文本块列表)。
    """
    day_dir = os.path.join(out_root, date_str)
    os.makedirs(day_dir, exist_ok=True)

    path_list, md_blocks = [], []
    for row in rows:
        extracted = extract_fault_metrics(row, data_root=data_root, prefer_zip=prefer_zip)
        md = render_markdown(row, extracted)

        fname = (f"{int(row['index']):03d}_{_safe_filename(row['object'])}_"
                 f"{_safe_filename(row['name'])}_{_safe_filename(row['fault_desrcibtion'])}.md")
        fpath = os.path.join(day_dir, fname)
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(md)
        path_list.append(fpath)
        md_blocks.append(md)

    # 当日汇总：便于按天导入向量库或人工通读
    merged_path = os.path.join(day_dir, f"all_incidents_{date_str}.md")
    with open(merged_path, "w", encoding="utf-8") as f:
        f.write(f"# AIOps 历史故障知识库（{date_str}，共 {len(rows)} 条）\n\n")
        f.write("\n".join(md_blocks))

    print(f"[OK] {date_str}: {len(path_list)} 条单条语料 + 当日汇总")
    return path_list, md_blocks


def build_corpus(date_str, fault_csv=None, out_root=None,
                 data_root=DEFAULT_DATA_ROOT, prefer_zip=False):
    """
    为单个日期构建语料，输出到 out_root/date_str/，返回单条 md 路径列表。
    该日期在故障表中无标注时返回空列表（不报错，兼容无故障日）。
    """
    fault_csv = fault_csv or os.path.join(data_root, "故障整理（预赛）.csv")
    out_root = out_root or os.path.join(PROJECT_DIR, CORPUS_DIRNAME)

    rows = load_faults(fault_csv, date_str)
    if not rows:
        print(f"[WARN] 故障表中没有 {date_str} 的故障标注")
        return []
    paths, _ = _build_one_day(date_str, rows, out_root, data_root, prefer_zip)
    return paths


def build_all_corpus(fault_csv=None, out_root=None,
                     data_root=DEFAULT_DATA_ROOT, prefer_zip=False):
    """
    全量构建：扫描所有可用日期，仅对故障表中有标注的日期生成语料，
    并在 out_root 下写一个跨日期总汇总文件。返回 {日期: 单条路径列表}。
    """
    fault_csv = fault_csv or os.path.join(data_root, "故障整理（预赛）.csv")
    out_root = out_root or os.path.join(PROJECT_DIR, CORPUS_DIRNAME)
    os.makedirs(out_root, exist_ok=True)

    report, all_blocks = [], []
    result_dict = {}
    for date_str in list_available_dates(data_root):
        rows = load_faults(fault_csv, date_str)
        if not rows:
            # 无故障标注的日期（如 04_20~04_23）不生成故障语料，
            # 但它们会由 build_baseline_corpus 转成正常基线
            print(f"[SKIP] {date_str}: 无故障标注（转由正常基线语料覆盖）")
            continue
        paths, blocks = _build_one_day(date_str, rows, out_root, data_root, prefer_zip)
        result_dict[date_str] = paths
        report.append((date_str, len(rows)))
        all_blocks.extend(blocks)

    # 跨日期总汇总
    total = sum(n for _, n in report)
    merged_path = os.path.join(out_root, "all_incidents.md")
    with open(merged_path, "w", encoding="utf-8") as f:
        f.write(f"# AIOps 历史故障知识库（全量，共 {total} 条，"
                f"覆盖 {len(report)} 天）\n\n")
        f.write("\n".join(all_blocks))

    print(f"[OK] 全量完成：共 {total} 条，总汇总 {merged_path}")
    return result_dict


# 命令行入口 Demo：python aiops_rag.py
if __name__ == "__main__":
    # 1. 全量构建故障语料：强制 zip 直读，验证未解压场景下所有日期都能打通
    build_all_corpus(prefer_zip=True)

    # 2. 全量构建正常基线语料：把无故障标注的日期也接入知识库
    build_baseline_corpus(prefer_zip=True)

    # 演示 query_metrics 工具（zip 直读模式）
    demo = query_metrics("docker_003", "container_cpu_used",
                         "2020/4/11 0:05", "2020_04_11", prefer_zip=True)
    print("\n[query_metrics zip 模式返回]")
    print(demo)
