"""测试固定数据（fixture）。

fixture 的来源与约定
--------------------
1. anomaly_data_example —— 模拟上游异常检测系统下发的数据，手工维护；
2. anomaly_understanding_example / plan_example / tool_results_example
   —— 用修正后的 anomaly_data 依次跑真实节点录制得到，**不要手写**。
   手写的 fixture 会和 tools.py 的实际返回结构脱节（曾经出现过
   status="normal" / cpu_usage=0.8 这种工具永远不会返回的值），
   那样测的是"模型能不能读懂人编的数据"，而不是流程本身。
   一致性由 test_agent.py::test_recorded_tool_results_match_live_run 守护。

anomaly_data 的自洽性约束
-------------------------
由 test_agent.py::test_anomaly_data_fixture_is_self_consistent 守护：
- window_start / window_end / generated_at / source_window_ids / 告警时间戳
  必须落在同一天。原数据的 window 是 2026-09-20，而 source_window_ids
  (LOG_20260921_*)、generated_at、get_alarm_history 返回的告警时间戳
  都是 2026-09-21——四处指向 09-21、只有 window 指向 09-20，
  说明是 window 自己写错了；
- root_cause_candidates 的概率之和必须为 1（原数据为 1.229，不是合法分布）。
"""

# ---------------------------------------------------------------------------
# 上游异常检测系统传入的标准化异常数据
# ---------------------------------------------------------------------------
anomaly_data_example = {
    "anomaly_data": {
        "event_id": "EVT_20260921_003",
        "device_id": "emss-pfc-payacctmcrsrv-subdomain",
        "window_index": None,
        # 窗口设为 09:00-09:08，使 get_alarm_history 返回的两条告警
        # (2026-09-21 09:04:12 / 09:05:31) 落在窗口内，形成完整证据
        "window_start": "2026-09-21T09:00:00Z",
        "window_end": "2026-09-21T09:08:00Z",
        "source_window_ids": [
            "LOG_20260921_277",
            "LOG_20260921_278",
            "LOG_20260921_279",
            "LOG_20260921_280",
            "LOG_20260921_281",
        ],
        "severity": "high",
        "anomaly_type": "log_anomaly",
        "root_cause_type": "network_congestion",
        "anomaly_score": 0.9923,
        "abnormal_features": ["log_latency", "log_volume", "warn_rate"],
        "feature_relations": [],
        # 概率归一化到 1（原数据之和为 1.229）
        "root_cause_candidates": [
            {"cause": "network_congestion", "probability": 0.638},
            {"cause": "device_failure", "probability": 0.227},
            {"cause": "link_failure", "probability": 0.070},
            {"cause": "routing_anomaly", "probability": 0.046},
            {"cause": "configuration_error", "probability": 0.019},
        ],
        "bn_evidence": {
            "cpu_abnormal": True,
            "rtt_abnormal": True,
            "packet_loss_abnormal": True,
        },
        "bn_evidence_provenance": {
            "cpu_abnormal": "event_metric:cpu",
            "rtt_abnormal": "event_metric:latency",
            "packet_loss_abnormal": "event_metric:packet_loss",
        },
        "model_version": None,
        "generated_at": "2026-09-21T09:20:00Z",
        "schema_version": "evidence/v1",
    }
}

# ---------------------------------------------------------------------------
# 异常理解节点结果（anomaly_understanding_node 真实输出）
# ---------------------------------------------------------------------------
anomaly_understanding_example = {
    "anomaly_understanding": {
        "anomaly_type": "log_anomaly",
        "summary": "2026-09-21 09:00:00Z 至 09:08:00Z，设备 emss-pfc-payacctmcrsrv-subdomain 发生高严重度日志异常，异常评分 0.9923。检测到日志延迟、日志量和告警率异常，并伴随 CPU、RTT、丢包指标异常证据。当前阶段仅进行异常理解与信息整理，不进行根因判断。",
        "anomaly_context": {
            "event_id": "EVT_20260921_003",
            "device_id": "emss-pfc-payacctmcrsrv-subdomain",
            "severity": "high",
            "time_window": {
                "start": "2026-09-21T09:00:00Z",
                "end": "2026-09-21T09:08:00Z",
            },
            "confidence": 0.9923,
        },
        "anomaly_pattern": "日志延迟升高、日志量异常、告警率异常；同时存在 CPU、RTT 和丢包异常信号，表现为日志指标与网络/资源指标联合异常，但因果关系尚未确认。",
        "affected_components": [
            "emss-pfc-payacctmcrsrv-subdomain（异常设备/服务实例）",
            "关联日志源窗口：LOG_20260921_277、LOG_20260921_278、LOG_20260921_279、LOG_20260921_280、LOG_20260921_281",
        ],
        "key_indicators": [
            "anomaly_type=log_anomaly",
            "severity=high",
            "anomaly_score=0.9923",
            "时间窗口：2026-09-21T09:00:00Z 至 2026-09-21T09:08:00Z",
            "source_window_ids 数量为 5：LOG_20260921_277、LOG_20260921_278、LOG_20260921_279、LOG_20260921_280、LOG_20260921_281",
            "abnormal_features：log_latency、log_volume、warn_rate",
            "bn_evidence：cpu_abnormal=true、rtt_abnormal=true、packet_loss_abnormal=true",
            "bn_evidence_provenance：cpu_abnormal 来自 event_metric:cpu；rtt_abnormal 来自 event_metric:latency；packet_loss_abnormal 来自 event_metric:packet_loss",
            "root_cause_candidates 包含 network_congestion、device_failure、link_failure、routing_anomaly、configuration_error，但均为待验证的模型候选，不能作为根因结论",
        ],
        "evidence_summary": "该事件为日志异常，检测到日志延迟、日志量和告警率异常，窗口内聚合了 5 个日志源窗口。BN 证据显示 CPU、RTT、丢包指标异常，但证据来源为事件指标，尚未与原始日志内容和原始指标时间序列交叉验证。当前证据可支持异常现象确认，但不足以确认根因。",
        "investigation_targets": [
            "查询 LOG_20260921_277 至 LOG_20260921_281 的原始日志内容，确认日志延迟、日志量和告警率的具体变化",
            "验证 event_metric:cpu 对应的 CPU 指标时间序列，确认异常幅度、持续时间和发生时间点",
            "验证 event_metric:latency 对应的 RTT/延迟指标时间序列，确认异常窗口内延迟变化",
            "验证 event_metric:packet_loss 对应的丢包指标时间序列，确认丢包率变化及分布",
            "核对 source_window_ids 与事件窗口的映射关系，确认日志窗口聚合逻辑",
            "检查 emss-pfc-payacctmcrsrv-subdomain 的网络拓扑、链路状态、接口流量和带宽利用率",
            "检查 emss-pfc-payacctmcrsrv-subdomain 的设备/实例健康状态、进程或容器资源使用情况",
            "检查相关服务上下游依赖、调用链和错误日志，确认影响范围",
            "检查近期配置变更、发布记录、路由/负载均衡变更和运维事件",
            "对比历史基线，确认当前日志和指标异常是否显著偏离正常范围",
            "补充查询是否存在同一时间窗口内其他关联设备或服务的异常事件",
        ],
        "investigation_priorities": [
            "优先验证原始日志与日志源窗口，确认日志异常事实和影响范围",
            "优先验证 CPU、RTT、丢包等事件指标时间序列，确认 BN 证据是否为真实持续异常",
            "检查网络链路、接口流量、带宽利用率和拓扑状态，验证网络侧异常信号",
            "检查设备/实例健康状态及进程、容器资源使用情况",
            "检查上下游调用链、依赖服务和错误日志，确认是否出现级联影响",
            "检查配置变更、发布记录和路由/负载均衡变更",
            "对比历史基线并关联其他同时段异常事件，缩小调查范围",
        ],
        "missing_information": [
            "window_index 缺失",
            "model_version 缺失",
            "feature_relations 为空",
            "缺少 LOG_20260921_277 至 LOG_20260921_281 的原始日志文本、告警详情和日志级别分布",
            "缺少 event_metric:cpu、event_metric:latency、event_metric:packet_loss 的原始时间序列数据、采样粒度和阈值定义",
            "缺少 emss-pfc-payacctmcrsrv-subdomain 的设备角色、服务依赖、部署位置和网络拓扑信息",
            "缺少近期配置变更、发布记录、路由变更和运维操作记录",
            "缺少正常状态下的日志量、日志延迟、告警率、CPU、RTT 和丢包基线",
            "缺少 root_cause_candidates 概率对应的模型版本、训练数据和判定依据",
            "缺少异常窗口内是否存在其他关联设备或服务的异常事件信息",
        ],
        "uncertainties": [
            "BN 证据为布尔异常结果，未提供具体数值、持续时间和异常起始时间，无法判断严重程度",
            "bn_evidence_provenance 仅说明证据来自 event_metric，尚未与原始指标和日志交叉验证",
            "root_cause_candidates 为模型输出概率，不能直接作为根因结论，仍需进一步调查验证",
            "日志异常与 CPU、RTT、丢包异常之间的因果关系未知，可能是同时发生的关联现象",
            "当前影响范围仅能基于 device_id 和日志源窗口推断，实际受影响组件和服务边界尚不明确",
            "高 anomaly_score 表示模型检测到的异常显著，但不代表根因已明确",
            "可能存在未纳入当前事件的其他关联组件、外部依赖或网络路径异常",
        ],
    }
}

# ---------------------------------------------------------------------------
# 任务规划节点结果（planner_node 真实输出）
# ---------------------------------------------------------------------------
plan_example = {
    "plan": [
        {
            "task_id": "task_1",
            "task": "查询异常设备 emss-pfc-payacctmcrsrv-subdomain 的运行状态，确认设备/实例健康状态、CPU、内存及设备状态，用于核对设备侧是否存在资源或健康异常。调查内容：设备运行状态与资源使用；数据类型：设备状态快照（CPU、内存、设备状态）。",
            "tool": "get_device_status",
            "arguments": {"device_id": "emss-pfc-payacctmcrsrv-subdomain"},
        },
        {
            "task_id": "task_2",
            "task": "验证 event_metric:cpu 证据，查询 emss-pfc-payacctmcrsrv-subdomain 的 CPU 指标时间序列，确认异常幅度、持续时间与发生时间点。调查内容：CPU 异常是否为真实持续异常；数据类型：CPU 指标时间序列（采样粒度、阈值待补充）。",
            "tool": "get_network_metrics",
            "arguments": {"device_id": "emss-pfc-payacctmcrsrv-subdomain", "metric": "cpu"},
        },
        {
            "task_id": "task_3",
            "task": "验证 event_metric:latency 证据，查询 emss-pfc-payacctmcrsrv-subdomain 的延迟/RTT 指标时间序列，确认异常窗口内延迟变化。调查内容：RTT 异常幅度与时间分布；数据类型：latency 指标时间序列。",
            "tool": "get_network_metrics",
            "arguments": {"device_id": "emss-pfc-payacctmcrsrv-subdomain", "metric": "latency"},
        },
        {
            "task_id": "task_4",
            "task": "验证 event_metric:packet_loss 证据，查询 emss-pfc-payacctmcrsrv-subdomain 的丢包指标时间序列，确认丢包率变化及分布。调查内容：丢包异常幅度与分布；数据类型：packet_loss 指标时间序列。",
            "tool": "get_network_metrics",
            "arguments": {"device_id": "emss-pfc-payacctmcrsrv-subdomain", "metric": "packet_loss"},
        },
        {
            "task_id": "task_5",
            "task": "查询 emss-pfc-payacctmcrsrv-subdomain 的接口流量/带宽利用率指标，用于核对网络侧是否存在拥塞或流量异常信号。调查内容：接口流量与带宽利用率；数据类型：traffic 指标时间序列。",
            "tool": "get_network_metrics",
            "arguments": {"device_id": "emss-pfc-payacctmcrsrv-subdomain", "metric": "traffic"},
        },
        {
            "task_id": "task_6",
            "task": "查询 emss-pfc-payacctmcrsrv-subdomain 在事件窗口 2026-09-21T09:00:00Z 至 2026-09-21T09:08:00Z 内的历史告警，核对告警率异常及同时段其他告警。调查内容：告警详情、告警级别、告警时间分布；数据类型：历史告警记录。",
            "tool": "get_alarm_history",
            "arguments": {
                "device_id": "emss-pfc-payacctmcrsrv-subdomain",
                "start_time": "2026-09-21T09:00:00Z",
                "end_time": "2026-09-21T09:08:00Z",
            },
        },
        {
            "task_id": "task_7",
            "task": "检索知识库中关于日志延迟升高、日志量异常、告警率异常及 CPU/RTT/丢包联合异常的已知故障模式与处理方案，用于形成调查假设（不作为根因结论）。调查内容：日志异常及关联指标的已知故障模式；数据类型：知识库故障原因与处置方案条目。",
            "tool": "search_network_knowledge",
            "arguments": {
                "query": "日志延迟升高 日志量异常 告警率异常 伴随 CPU 升高 RTT 丢包 联合异常的故障模式与排查方案"
            },
        },
        {
            "task_id": "task_8",
            "task": "查询 emss-pfc-payacctmcrsrv-subdomain 的链路状态，用于核对网络侧链路异常信号。说明：缺少对端设备信息（属 missing_information 中的拓扑未知项），target_device 待根据拓扑信息补充后才能准确查询，此处占位待确认。",
            "tool": "get_link_status",
            "arguments": {
                "source_device": "emss-pfc-payacctmcrsrv-subdomain",
                "target_device": "",
            },
        },
    ]
}

# ---------------------------------------------------------------------------
# 工具执行结果（tool_execution_node(plan_example) 的真实输出，勿手写）
# ---------------------------------------------------------------------------
tool_results_example = {
    "tool_results": [
        {
            "task_id": "task_1",
            "task": "查询异常设备 emss-pfc-payacctmcrsrv-subdomain 的运行状态，确认设备/实例健康状态、CPU、内存及设备状态，用于核对设备侧是否存在资源或健康异常。调查内容：设备运行状态与资源使用；数据类型：设备状态快照（CPU、内存、设备状态）。",
            "tool": "get_device_status",
            "arguments": {"device_id": "emss-pfc-payacctmcrsrv-subdomain"},
            "success": True,
            "result": {
                "device_id": "emss-pfc-payacctmcrsrv-subdomain",
                "status": "running",
                "cpu_usage": 92.5,
                "memory_usage": 71.2,
            },
            "error": None,
        },
        {
            "task_id": "task_2",
            "task": "验证 event_metric:cpu 证据，查询 emss-pfc-payacctmcrsrv-subdomain 的 CPU 指标时间序列，确认异常幅度、持续时间与发生时间点。调查内容：CPU 异常是否为真实持续异常；数据类型：CPU 指标时间序列（采样粒度、阈值待补充）。",
            "tool": "get_network_metrics",
            "arguments": {"device_id": "emss-pfc-payacctmcrsrv-subdomain", "metric": "cpu"},
            "success": True,
            # get_network_metrics 的 mock 只实现了 latency/packet_loss/traffic，
            # metric="cpu" 会返回 null。工具"成功"但没数据——这是最容易骗过诊断节点的失败模式
            "result": {
                "device_id": "emss-pfc-payacctmcrsrv-subdomain",
                "metric": "cpu",
                "result": None,
            },
            "error": None,
        },
        {
            "task_id": "task_3",
            "task": "验证 event_metric:latency 证据，查询 emss-pfc-payacctmcrsrv-subdomain 的延迟/RTT 指标时间序列，确认异常窗口内延迟变化。调查内容：RTT 异常幅度与时间分布；数据类型：latency 指标时间序列。",
            "tool": "get_network_metrics",
            "arguments": {"device_id": "emss-pfc-payacctmcrsrv-subdomain", "metric": "latency"},
            "success": True,
            "result": {
                "device_id": "emss-pfc-payacctmcrsrv-subdomain",
                "metric": "latency",
                "result": {"value": 185, "unit": "ms"},
            },
            "error": None,
        },
        {
            "task_id": "task_4",
            "task": "验证 event_metric:packet_loss 证据，查询 emss-pfc-payacctmcrsrv-subdomain 的丢包指标时间序列，确认丢包率变化及分布。调查内容：丢包异常幅度与分布；数据类型：packet_loss 指标时间序列。",
            "tool": "get_network_metrics",
            "arguments": {"device_id": "emss-pfc-payacctmcrsrv-subdomain", "metric": "packet_loss"},
            "success": True,
            "result": {
                "device_id": "emss-pfc-payacctmcrsrv-subdomain",
                "metric": "packet_loss",
                "result": {"value": 8.2, "unit": "%"},
            },
            "error": None,
        },
        {
            "task_id": "task_5",
            "task": "查询 emss-pfc-payacctmcrsrv-subdomain 的接口流量/带宽利用率指标，用于核对网络侧是否存在拥塞或流量异常信号。调查内容：接口流量与带宽利用率；数据类型：traffic 指标时间序列。",
            "tool": "get_network_metrics",
            "arguments": {"device_id": "emss-pfc-payacctmcrsrv-subdomain", "metric": "traffic"},
            "success": True,
            "result": {
                "device_id": "emss-pfc-payacctmcrsrv-subdomain",
                "metric": "traffic",
                "result": {"value": 860, "unit": "Mbps"},
            },
            "error": None,
        },
        {
            "task_id": "task_6",
            "task": "查询 emss-pfc-payacctmcrsrv-subdomain 在事件窗口 2026-09-21T09:00:00Z 至 2026-09-21T09:08:00Z 内的历史告警，核对告警率异常及同时段其他告警。调查内容：告警详情、告警级别、告警时间分布；数据类型：历史告警记录。",
            "tool": "get_alarm_history",
            "arguments": {
                "device_id": "emss-pfc-payacctmcrsrv-subdomain",
                "start_time": "2026-09-21T09:00:00Z",
                "end_time": "2026-09-21T09:08:00Z",
            },
            "success": True,
            # 两条告警落在事件窗口 09:00-09:08 内，可作为窗口内的直接证据
            "result": {
                "device_id": "emss-pfc-payacctmcrsrv-subdomain",
                "start_time": "2026-09-21T09:00:00Z",
                "end_time": "2026-09-21T09:08:00Z",
                "alarms": [
                    {"timestamp": "2026-09-21 09:04:12", "type": "high_cpu", "severity": "warning"},
                    {"timestamp": "2026-09-21 09:05:31", "type": "high_latency", "severity": "critical"},
                ],
            },
            "error": None,
        },
        {
            "task_id": "task_7",
            "task": "检索知识库中关于日志延迟升高、日志量异常、告警率异常及 CPU/RTT/丢包联合异常的已知故障模式与处理方案，用于形成调查假设（不作为根因结论）。调查内容：日志异常及关联指标的已知故障模式；数据类型：知识库故障原因与处置方案条目。",
            "tool": "search_network_knowledge",
            "arguments": {
                "query": "日志延迟升高 日志量异常 告警率异常 伴随 CPU 升高 RTT 丢包 联合异常的故障模式与排查方案"
            },
            "success": True,
            "result": {
                "query": "日志延迟升高 日志量异常 告警率异常 伴随 CPU 升高 RTT 丢包 联合异常的故障模式与排查方案",
                "documents": [
                    {"title": "高延迟故障排查", "content": "高延迟可能与链路拥塞、CPU负载过高或丢包有关。"},
                    {"title": "网络拥塞案例", "content": "带宽利用率持续超过90%可能造成明显排队延迟。"},
                ],
            },
            "error": None,
        },
        {
            "task_id": "task_8",
            "task": "查询 emss-pfc-payacctmcrsrv-subdomain 的链路状态，用于核对网络侧链路异常信号。说明：缺少对端设备信息（属 missing_information 中的拓扑未知项），target_device 待根据拓扑信息补充后才能准确查询，此处占位待确认。",
            "tool": "get_link_status",
            "arguments": {
                "source_device": "emss-pfc-payacctmcrsrv-subdomain",
                "target_device": "",
            },
            "success": True,
            # bandwidth_utilization=91.3% 超过知识库中 90% 的拥塞阈值，
            # 是本次唯一能直接支撑 network_congestion 的现场指标
            "result": {
                "source_device": "emss-pfc-payacctmcrsrv-subdomain",
                "target_device": "",
                "status": "degraded",
                "latency": 170,
                "packet_loss": 7.8,
                "bandwidth_utilization": 91.3,
            },
            "error": None,
        },
    ]
}

# ---------------------------------------------------------------------------
# 诊断节点的完整输入：同时需要 anomaly_data 与 tool_results
# ---------------------------------------------------------------------------
diagnosis_state_example = {**anomaly_data_example, **tool_results_example}

diagnosis_results_example = {
    'diagnosis': {'root_cause': 'network_congestion', 'root_cause_category': 'network_congestion', 'confidence': 0.6,
                  'reasoning': "事件窗口 2026-09-21T09:00:00Z~09:08:00Z 内，emss-pfc-payacctmcrsrv-subdomain 出现日志延迟升高、日志量异常、告警率异常（anomaly_score 0.9923）。客观测量显示：RTT=185ms（task_3）、丢包率=8.2%（task_4）、链路状态为 degraded 且带宽利用率=91.3%（task_8），与知识库'带宽利用率持续超过90%可造成明显排队延迟''高延迟可能与链路拥塞、CPU负载过高或丢包有关'（task_7）一致，形成'网络拥塞导致排队延迟与丢包，进而造成日志上报延迟与告警率升高'的推理链。但该结论存在两点未闭合：其一，link_status 查询的 target_device 为空（占位调用），结果的可归属性未经拓扑验证；其二，CPU 使用率 92.5% 且 09:04:12 出现 high_cpu 告警，早于 09:05:31 的 high_latency 告警，CPU 过载既可能是拥塞的结果也可能是延迟/丢包的独立成因（知识库亦将 CPU 负载过高列为高延迟并列原因）。因此判定 network_congestion 为当前证据下最受支持的方向，但置信度仅 0.6，不排除 device_failure 为并发或上游成因，需按 verification_steps 进一步区分因果方向。",
                  'candidate_verdicts': [{'cause': 'network_congestion', 'verdict': 'supported',
                                          'evidence': 'task_8 get_link_status 返回 status=degraded、latency=170ms、packet_loss=7.8%、bandwidth_utilization=91.3%；task_3 latency=185ms；task_4 packet_loss=8.2%；task_7 知识库指出带宽利用率持续>90%可造成排队延迟。多项独立指标方向一致。',
                                          'posterior': 0.62},
                                         {'cause': 'device_failure', 'verdict': 'insufficient_evidence',
                                          'evidence': 'task_1 显示 status=running、cpu_usage=92.5%、memory_usage=71.2%，设备未宕机；task_6 存在 high_cpu（warning，09:04:12）告警；但 task_2 CPU 时间序列返回 null，无法确认 CPU 高位的幅度、起始时间与持续性，也无磁盘/进程/重启类证据，不足以判定设备故障。',
                                          'posterior': 0.22}, {'cause': 'link_failure', 'verdict': 'refuted',
                                                               'evidence': 'task_8 链路状态为 degraded 而非 down，且仍可测到 latency=170ms、packet_loss=7.8% 的流量特征，说明链路未中断；无链路中断/端口 down 记录。',
                                                               'posterior': 0.05},
                                         {'cause': 'routing_anomaly', 'verdict': 'insufficient_evidence',
                                          'evidence': '本次工具执行未采集路由表、路由变更事件或路径探测数据，无任何支持或反驳该假设的客观证据。',
                                          'posterior': 0.05},
                                         {'cause': 'configuration_error', 'verdict': 'insufficient_evidence',
                                          'evidence': '本次工具执行未获取配置快照、变更审计记录或配置比对结果，无证据表明窗口前后存在配置变更；task_1 设备状态正常亦不排除静默配置问题。',
                                          'posterior': 0.06}], 'counter_evidence': [
            "task_1 与 task_6 表明 CPU 使用率高达 92.5% 并触发 high_cpu 告警，且该告警时间（09:04:12）早于 high_latency 告警（09:05:31），存在'设备侧资源过载为因、延迟/丢包为果'的替代解释，与 network_congestion 假设方向相反。",
            "task_7 知识库明确将'CPU 负载过高'与'链路拥塞'并列为高延迟的可能原因，说明现有证据无法唯一指向网络拥塞。",
            'task_5 返回 traffic=860Mbps，但未提供链路带宽容量基线，无法据此独立计算带宽利用率，拥塞结论主要依赖 task_8 的 91.3% 单值。',
            'task_8 的调用参数 target_device 为空字符串，属占位调用，其结果的可归属性未经拓扑验证，作为拥塞核心证据存在可靠性折扣。',
            'task_2 的 CPU 指标时间序列返回 null，说明部分指标采集缺失，整体证据完整性不足。'], 'unresolved': [
            'CPU 高位（92.5%）与网络拥塞之间的因果方向未确定：是拥塞引发设备处理/上报积压，还是 CPU 过载导致网络栈处理延迟与丢包。',
            'task_2 的 CPU 时间序列为何返回 null（采集缺失、指标未注册或设备未上报），导致 CPU 异常的持续时间与起始时刻未知。',
            '链路带宽容量与实际利用率基线缺失，860Mbps 是否已达到或超过链路容量无法判定，91.3% 利用率的可信区间未知。',
            '缺少对端设备与拓扑信息，task_8 的 degraded/91.3% 结果无法确认归属于哪条链路、是否有上游设备共同异常。',
            '日志延迟升高（log_latency）究竟是网络传输延迟导致，还是设备侧日志采集/上报组件自身排队所致，未做区分。',
            '异常窗口 09:00~09:08 前后是否发生流量突增（是否存在外部流量源或批量任务），未采集流量时间序列。',
            '是否存在并发的路由变更或配置变更，未采集相关审计数据。'], 'verification_steps': [
            '补充采集 CPU 指标时间序列（并排查 task_2 返回 null 的原因），确认 92.5% 是瞬时峰值还是窗口内持续高位，明确 CPU 异常与延迟升高的时间先后。',
            '查询该设备接口/链路的带宽容量及 09:00~09:08 的利用率时间曲线，验证带宽利用率是否持续 >90%，确认拥塞的持续性与峰值。',
            '补全拓扑信息后重新执行 get_link_status（指定真实 target_device），核实链路 degraded 状态与 91.3%/7.8%/170ms 的真实归属链路。',
            '采集同时间段相邻设备与上游链路的 latency、packet_loss、traffic 指标，判断异常范围是单点还是链路级，以支持或排除网络拥塞。',
            '拉取 09:00~09:08 的流量时间序列，核对是否存在流量突增作为拥塞诱因。',
            '检查设备侧日志采集/上报组件（agent）自身队列与丢弃计数，区分 log_latency 来源是网络还是本地处理积压，并解释 high_cpu 的成因归属。',
            '调取路由表与路由变更事件，排除 routing_anomaly。',
            '调取窗口前后的配置变更审计记录，排除 configuration_error。',
            '由上述结果重新评估 device_failure 与 network_congestion 的相对概率，若 CPU 长时间饱和且网络利用率未达阈值，则需将根因修正为设备侧资源故障。']},
    'evidence_chain': [
        {'step': 1,
         'phenomenon': '事件窗口内 emss-pfc-payacctmcrsrv-subdomain 出现日志延迟升高、日志量异常、告警率异常，anomaly_score=0.9923，severity=high，BN 初始证据标记 cpu_abnormal/rtt_abnormal/packet_loss_abnormal 均为 true。',
         'action': '触发设备运行状态与资源使用查询（task_1 get_device_status）。',
         'observation': 'status=running，cpu_usage=92.5%，memory_usage=71.2%。',
         'inference': '设备未宕机，可排除 outright 设备失效；但 CPU 处于高位存在资源压力，内存正常。CPU 高位与 RTT/丢包异常可能互为因果，需进一步区分方向。'},
        {'step': 2, 'phenomenon': '初始证据含 rtt_abnormal，且现象层出现日志延迟升高。',
         'action': '查询 latency 指标时间序列（task_3 get_network_metrics metric=latency）。',
         'observation': '返回 latency=185ms（单位 ms），为单点值，无时间序列。',
         'inference': '存在明显延迟异常，与日志延迟升高现象方向一致，符合网络拥塞或设备处理变慢的特征；但因仅单点值，无法判断延迟的持续性与变化趋势。'},
        {'step': 3, 'phenomenon': '初始证据含 packet_loss_abnormal。',
         'action': '查询丢包指标（task_4 get_network_metrics metric=packet_loss）。',
         'observation': '返回 packet_loss=8.2%（单位 %）。',
         'inference': '存在显著丢包，与高延迟同时出现，符合链路拥塞或链路质量劣化的特征；8.2% 丢包亦会反过来推高重传与 RTT。'},
        {'step': 4, 'phenomenon': '需核对网络侧是否存在拥塞/流量异常信号。',
         'action': '查询接口流量/带宽利用率指标（task_5 get_network_metrics metric=traffic）。',
         'observation': '返回 traffic=860Mbps，未返回链路带宽容量或利用率。',
         'inference': '无法凭该值独立判定是否拥塞，因缺少容量基线；该结果只能作为背景流量量级，不能作为拥塞支持或反驳证据。'},
        {'step': 5, 'phenomenon': '现象层包含 warn_rate 告警率异常，需核对窗口内实际告警。',
         'action': '查询 09:00:00Z~09:08:00Z 历史告警（task_6 get_alarm_history）。',
         'observation': '返回两条告警：09:04:12 high_cpu（warning）、09:05:31 high_latency（critical）。',
         'inference': '告警与异常窗口吻合，验证了告警率异常。时间顺序上 CPU 告警早于延迟告警，提示 CPU 升高可能先于或伴随延迟升高，但不能据此确定因果方向，构成对 network_congestion 结论的反向线索。'},
        {'step': 6, 'phenomenon': '需要已知故障模式辅助形成调查假设。',
         'action': '检索知识库（task_7 search_network_knowledge）。',
         'observation': "返回'高延迟可能与链路拥塞、CPU负载过高或丢包有关'、'带宽利用率持续超过90%可能造成明显排队延迟'。",
         'inference': "知识库同时支持'链路拥塞'与'CPU 过载'两条并列路径，因此当前证据不能唯一指向网络拥塞，必须保留 device_failure 备选假设。"},
        {'step': 7, 'phenomenon': '需核对网络侧链路异常信号以区分拥塞与链路故障。',
         'action': '查询链路状态（task_8 get_link_status，source_device=emss-pfc-payacctmcrsrv-subdomain，target_device 为空占位）。',
         'observation': 'status=degraded，latency=170ms，packet_loss=7.8%，bandwidth_utilization=91.3%。',
         'inference': '链路未中断但处于降级态，带宽利用率 91.3% 超过知识库 90% 排队延迟阈值，与高延迟、高丢包一致，支持 network_congestion；同时排除 link_failure（非 down）。但 target_device 为空，该结果的可归属性未经拓扑验证，作为核心证据需降权。'},
        {'step': 8, 'phenomenon': '需要对 CPU 异常的幅度与持续性做量化验证。',
         'action': '查询 CPU 指标时间序列（task_2 get_network_metrics metric=cpu）。',
         'observation': 'result 为 null，未返回任何采样点。',
         'inference': 'CPU 异常未获得时间序列验证，92.5% 的持续时长、起始时刻与峰值均未知，故 device_failure 方向只能判定为 insufficient_evidence；同时指标采集缺失本身是需要排查的数据质量问题。'}],
    'confidence': 0.6}

validation_node_state_example = {**diagnosis_results_example, **diagnosis_state_example}