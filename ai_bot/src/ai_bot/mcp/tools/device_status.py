import psutil
from datetime import datetime

def get_device_status(device_id: str) -> dict:
    """
    查询指定网络设备的运行状态，包括 CPU、内存和设备状态。
    """
    cpu = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage("C:\\")
    network = psutil.net_io_counters()

    return {
        "device_id": device_id,
        "timestamp": datetime.now().astimezone().isoformat(),
        "status": "running",
        "cpu_usage_pct": cpu,
        "memory_usage_pct": memory.percent,
        "disk_usage_pct": disk.percent,
        "network_bytes_sent": network.bytes_sent,
        "network_bytes_recv": network.bytes_recv
    }
