import asyncio
import json
import os
from playwright.async_api import async_playwright

PROFILES_JSON_PATH = 'profiles_linkedin.json'
BASE_PROFILE_PATH = r'C:\Users\Sage\PycharmProjects\MasterScraper\profiles'


def load_profiles():
    if os.path.exists(PROFILES_JSON_PATH) and os.path.getsize(PROFILES_JSON_PATH) > 0:
        with open(PROFILES_JSON_PATH, 'r') as f:
            return json.load(f)
    return {}


async def run_profile(playwright, profile_name: str) -> None:
    profiles = load_profiles()
    if profile_name not in profiles:
        print(f"Profile '{profile_name}' not found.")
        return

    profile = profiles[profile_name]
    profile_path = os.path.join(BASE_PROFILE_PATH, f"Profile {profile['profile_number']}")

    browser_args = {}

    if profile['proxy']:
        proxy_parts = profile['proxy'].split(':')
        if len(proxy_parts) == 4:
            browser_args['proxy'] = {
                "server": f"{proxy_parts[0]}:{proxy_parts[1]}",
                "username": proxy_parts[2],
                "password": proxy_parts[3]
            }
        else:
            print(f"Invalid proxy format for profile '{profile_name}'. Proceeding without proxy.")

    browser = await playwright.firefox.launch_persistent_context(
        user_data_dir=profile_path,
        headless=False,
        **browser_args
    )
    page = await browser.new_page()
    await page.goto("https://www.linkedin.com/")

    print(f"Profile loaded: {profile_name}")
    print(f"Browser path: {profile_path}")
    print(f"Proxy: {profile['proxy'] if 'proxy' in browser_args else 'None'}")

    await page.pause()


async def main():
    profiles = load_profiles()
    if not profiles:
        print("No profiles found. Please create a profile first.")
        return

    print("Available profiles:")
    for profile_name in profiles:
        print(f"- {profile_name}")

    profile_name = input("Enter the profile name to load: ")

    async with async_playwright() as playwright:
        await run_profile(playwright, profile_name)


if __name__ == "__main__":
    asyncio.run(main())