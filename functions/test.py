import asyncio
from playwright.async_api import async_playwright


async def main():
    url = input("Enter the URL to visit: ")
    proxy_ip_port = input("Enter proxy IP:port (e.g., 192.168.1.1:8080): ")
    proxy_username = input("Enter proxy username: ")
    proxy_password = input("Enter proxy password: ")

    proxy_ip, proxy_port = proxy_ip_port.split(':')

    async with async_playwright() as p:
        browser_args = {
            'proxy': {
                "server": f"http://{proxy_ip}:{proxy_port}",
                "username": proxy_username,
                "password": proxy_password
            }
        }

        print(f"Launching browser with proxy: {proxy_ip}:{proxy_port}")
        print(f"Proxy credentials: {proxy_username}:{proxy_password}")

        try:
            browser = await p.firefox.launch(headless=False, **browser_args)
            page = await browser.new_page()

            print(f"Navigating to {url}")
            await page.goto(url)

            # Keep the browser open until the user presses Enter
            input("Press Enter to close the browser...")
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())