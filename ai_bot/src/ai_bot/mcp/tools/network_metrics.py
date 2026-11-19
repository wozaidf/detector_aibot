def get_network_metrics(device_id: str, metric: str) -> dict:
    """
    查询指定设备的网络性能指标。

    Args:
        device_id: 设备ID，例如 router-001
        metric: 指标名称，例如 latency、packet_loss、traffic
    """
    mock_data = {
        "latency": {
            "value": 185,
            "unit": "ms"
        },
        "packet_loss": {
            "value": 8.2,
            "unit": "%"
        },
        "traffic": {
            "value": 860,
            "unit": "Mbps"
        }
    }

    return {
        "device_id": device_id,
        "metric": metric,
        "result": mock_data.get(metric)
    }
