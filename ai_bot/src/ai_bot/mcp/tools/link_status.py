def get_link_status(
        source_device: str,
        target_device: str
) -> dict:
    """
    查询两个网络设备之间的链路状态。
    """
    return {
        "source_device": source_device,
        "target_device": target_device,
        "status": "degraded",
        "latency": 170,
        "packet_loss": 7.8,
        "bandwidth_utilization": 91.3
    }