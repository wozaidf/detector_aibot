import sys

from langchain_mcp_adapters.client import MultiServerMCPClient

client = MultiServerMCPClient({
    "network": {
        "transport": "stdio",
        "command": sys.executable,
        "args": ["-m", "ai_bot.mcp.mcp_server"]
    }
})


async def get_mcp_tools():
    tools = await client.get_tools()

    tool_map = {
        tool.name: tool
        for tool in tools
    }

    return tool_map
