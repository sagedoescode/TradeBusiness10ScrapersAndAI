import asyncio
from playwright.async_api import async_playwright
from useful import verify_proxy

async def test_proxy(proxy_string):
    async with async_playwright() as p:
        browser_args = {}

        # Parse and set up the proxy
        proxy_parts = proxy_string.split(':')
        if len(proxy_parts) == 4:
            browser_args['proxy'] = {
                "server": f"{proxy_parts[0]}:{proxy_parts[1]}",
                "username": proxy_parts[2],
                "password": proxy_parts[3]
            }
        elif len(proxy_parts) == 2:
            browser_args['proxy'] = {
                "server": proxy_string
            }
        else:
            print("Invalid proxy format. Please use 'ip:port' or 'ip:port:username:password'")
            return

        # Launch the browser with the specified proxy
        browser = await p.firefox.launch(headless=False, **browser_args)
        page = await browser.new_page()

        # Navigate to whatismyip.com
        await page.goto("https://www.whatismyip.com/")

        # Wait for user input before closing the browser
        input("Press Enter to close the browser...")

        await browser.close()


async def main():
    proxy_string = input("Enter the proxy string (ip:port or ip:port:username:password): ")
    await test_proxy(proxy_string)


if __name__ == "__main__":
    asyncio.run(main())