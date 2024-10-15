import random

import aiohttp
from playwright.async_api import async_playwright
async def verify_proxy(proxy):
    proxy_url = f"http://{proxy['username']}:{proxy['password']}@{proxy['ip']}:{proxy['port']}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get('http://httpbin.org/ip', proxy=proxy_url, timeout=10) as response:
                if response.status == 200:
                    return True
        except:
            pass
    return False

async def get_verified_proxy(proxies):
    while True:
        proxy = random.choice(proxies)
        if await verify_proxy(proxy):
            return proxy
        print(f"Proxy {proxy['ip']}:{proxy['port']} failed verification, trying another...")
def parse_proxy_file(file_path):
    proxies = []
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split(':')
            if len(parts) == 4:
                proxy = {
                    'ip': parts[0],
                    'port': parts[1],
                    'username': parts[2],
                    'password': parts[3]
                }
                proxies.append(proxy)
    return proxies

async def example():
    proxy_path = r'proxies.txt'
    all_proxies = parse_proxy_file(proxy_path)
    proxy = await get_verified_proxy(all_proxies)
    print(f"Worker using Proxy: {proxy}")

    browser_args = {
        'headless': True,
        'proxy': {
            "server": f"{proxy['ip']}:{proxy['port']}",
            "username": proxy['username'],
            "password": proxy['password']
        }
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(**browser_args)
        context = await browser.new_context(viewport={"width": 1280, "height": 1080})
        page = await context.new_page()


        await page.goto("https://www.thumbtack.com/", timeout=30000)

        await page.pause()

