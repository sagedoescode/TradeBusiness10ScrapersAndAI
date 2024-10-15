import aiohttp
import asyncio
import json
import base64

BASE_URL = "http://192.168.2.13:10101/api"
USERNAME = "sagedoes"
PASSWORD = "code123"

# Create the authentication header
AUTH_HEADER = {
    "Authorization": "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
}

async def get_us_proxy():
    async with aiohttp.ClientSession(headers=AUTH_HEADER) as session:
        async with session.get(f"{BASE_URL}/proxy?num=1&country=US") as response:
            if response.status == 200:
                data = await response.json()
                return data[0] if data else None
    return None

async def forward_proxy_to_port(proxy, port):
    async with aiohttp.ClientSession(headers=AUTH_HEADER) as session:
        url = f"{BASE_URL}/forward?id={proxy['id']}&port={port}"
        async with session.get(url) as response:
            return response.status == 200

async def check_port_status(port):
    async with aiohttp.ClientSession(headers=AUTH_HEADER) as session:
        url = f"{BASE_URL}/port_status?ports={port}"
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data.get(str(port), {}).get("proxy_id") is not None
    return False

async def test_proxy(proxy_url):
    try:
        proxy_auth = aiohttp.BasicAuth(USERNAME, PASSWORD)
        async with aiohttp.ClientSession() as session:
            async with session.get("https://httpbin.org/ip", proxy=proxy_url, proxy_auth=proxy_auth, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("origin") != ""
    except:
        pass
    return False

async def main():
    start_port = 60000
    num_ports = 100

    for port in range(start_port, start_port + num_ports):
        # Check if port already has a proxy
        if await check_port_status(port):
            print(f"Port {port} already has a proxy assigned.")
            continue

        # Get a US proxy
        proxy = await get_us_proxy()
        if not proxy:
            print(f"Failed to get a US proxy for port {port}.")
            continue

        # Forward proxy to port
        if await forward_proxy_to_port(proxy, port):
            print(f"Successfully forwarded proxy to port {port}")
        else:
            print(f"Failed to forward proxy to port {port}")
            continue

        # Test the proxy
        proxy_url = f"http://{proxy['host']}:{proxy['port']}"
        if await test_proxy(proxy_url):
            print(f"Proxy test successful for port {port}: {proxy_url}")
        else:
            print(f"Proxy test failed for port {port}: {proxy_url}")

if __name__ == "__main__":
    asyncio.run(main())