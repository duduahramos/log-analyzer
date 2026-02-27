from typing import Any

import asyncio
import httpx
from mcp.server.fastmcp import FastMCP


mcp = FastMCP("ProductLogs", host="127.0.0.1", port=1234)


OPENSEARCH_API_BASE = "http://localhost:9200"
USER_AGENT = "log_analyzer_llm/1.0"
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json"
}


async def make_os_request(url: str) -> dict | None:
    async with httpx.AsyncClient() as httpClient:
        try:
            response = await httpClient.get(url, headers=HEADERS, timeout=30.0)
            response.raise_for_status() # if status >= 400, throw exception
            
            return response.json()
        except Exception as ex:
            print(f"Error: {ex}")

            return None


def format_health_check(response: dict) -> str:
    status_emoji = {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(response["status"], "⚪")

    return (
        f"{status_emoji} Cluster: {response['cluster_name']} ({response['status']})\n"
        f"Nós: {response['number_of_nodes']} | "
        f"Shards ativos: {response['active_shards']} | "
        f"Não alocados: {response['unassigned_shards']}\n"
        f"Tarefas pendentes: {response['number_of_pending_tasks']}"
    )


def format_count(response: dict) -> str:
    return str(response["count"])


@mcp.tool()
async def get_cluster_health_status() -> str:
    """status da porra do servidor"""
    url = f"{OPENSEARCH_API_BASE}/_cluster/health"
    health_response = await make_os_request(url)

    if not health_response:
        return "Unable to get health status."

    data_formated = format_health_check(health_response)
    
    return data_formated


@mcp.tool()
async def get_documents_count_by_index_name(index_name: str) -> str:
    """Get document count by index name in opensearch

    Args:
        index_name (str): name of the index in opensearch
    """
    
    url = f"{OPENSEARCH_API_BASE}/{index_name}/_count"
    count_response = await make_os_request(url)
    
    if not count_response:
        return f"Unable to count {index_name} documents"

    response_formated = format_count(count_response)
    
    return response_formated


def main(): 
    # mcp.run(transport="stdio")
    mcp.run(transport="sse")

if __name__ == "__main__":
    main()