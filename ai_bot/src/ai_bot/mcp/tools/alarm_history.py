def get_alarm_history(
        device_id: str,
        start_time: str,
        end_time: str
) -> dict:
    """
    查询指定设备在给定时间范围内的历史告警。
    """
    return {
        "device_id": device_id,
        "start_time": start_time,
        "end_time": end_time,
        "alarms": [
            {
                "timestamp": "2026-09-21 09:04:12",
                "type": "high_cpu",
                "severity": "warning"
            },
            {
                "timestamp": "2026-09-21 09:05:31",
                "type": "high_latency",
                "severity": "critical"
            }
        ]
    }