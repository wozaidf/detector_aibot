from fastmcp import FastMCP
from ai_bot.mcp.tools.device_status import get_device_status
from ai_bot.mcp.tools.link_status import get_link_status
from ai_bot.mcp.tools.alarm_history import get_alarm_history
from ai_bot.mcp.tools.network_metrics import get_network_metrics

# 所有可用工具
tools = [
    get_device_status,
    get_network_metrics,
    get_alarm_history,
    get_link_status,
]

mcp = FastMCP("My_Mcp_Server", instructions="提供用于网络设备状态查询的工具")

# 注册工具
for tool in tools:
    mcp.tool(tool)

if __name__ == "__main__":
    mcp.run()
