# ! pip install selenium-wire blinker==1.7.0
import random
import aiohttp
from selenium import webdriver
from seleniumwire import webdriver as wb
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from time import sleep
import asyncio
import pandas as pd
from datetime import datetime
import os
from selenium.webdriver.common.keys import Keys
import csv

# Constants for file paths


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


async def load_progress(PROGRESS_FILE_PATH):
    if os.path.exists(PROGRESS_FILE_PATH):
        with open(PROGRESS_FILE_PATH, 'r') as file:
            return set(line.strip() for line in file)
    return set()


async def save_progress(niche, zip_code, PROGRESS_FILE_PATH):
    async with asyncio.Lock():
        with open(PROGRESS_FILE_PATH, 'a') as file:
            file.write(f"{niche}:{zip_code}\n")


async def save_to_csv(row, OUTPUT_CSV_PATH):
    fieldnames = ['Niche', 'Zip Code', 'Name', 'Instagram', 'Facebook', 'Credentials_Name', 'Credentials_Licence',
                  'Email', 'Phone']

    file_exists = os.path.isfile(OUTPUT_CSV_PATH)

    async with asyncio.Lock():
        mode = 'a' if file_exists else 'w'
        with open(OUTPUT_CSV_PATH, mode, newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)


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

def random_sleep(): sleep(random.randint(4,6))

async def get_proxy():
    proxy_path = r"C:\Users\Sage\PycharmProjects\MasterScraper\proxies/proxies.txt"
    all_proxies = parse_proxy_file(proxy_path)
    proxy = await get_verified_proxy(all_proxies)
    print(f"Worker using Proxy: {proxy}")

    return proxy

def get_sleep_interval(interval_type):
    if interval_type == "tiny":
        return random.uniform(0.5, 1.5)
    elif interval_type == "short":
        return random.uniform(2.5, 3.5)
    elif interval_type == "medium":
        return random.uniform(3, 5)
    elif interval_type == "long":
        return random.uniform(5, 8)
    else:
        return 2

def human_typing(element, text):
    for char in text:
        element.send_keys(char)
        sleep(get_sleep_interval("tiny"))

async def process_niche_zip(niche, zip_code, progress, OUTPUT_FILE_PATH, PROGRESS_FILE_PATH):
    if f"{niche}:{zip_code}" in progress:
        print(f"Skipping {niche} in {zip_code} - already processed")
        return

    proxy = await get_proxy()

    chrome_options = Options()
    chrome_options.add_argument(f'--proxy-server={proxy["ip"]}:{proxy["port"]}')
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument(
        'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36')
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.binary_location = r'C:\Users\Sage\PycharmProjects\MasterScraper\functions\chrome-win64\chrome.exe'

    options = {
        'proxy': {
            'http': f'http://{proxy["username"]}:{proxy["password"]}@{proxy["ip"]}:{proxy["port"]}',
            'https': f'https://{proxy["username"]}:{proxy["password"]}@{proxy["ip"]}:{proxy["port"]}',
            'no_proxy': 'localhost,127.0.0.1'
        }
    }

    driver = wb.Chrome(service=Service(os.path.join(os.getcwd(), 'chromedriver.exe')), options=chrome_options,
                       seleniumwire_options=options)

    try:
        driver.get("https://www.thumbtack.com/")
        sleep(7)

        search_menu = WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.XPATH, "//input[@role='combobox' and @aria-label='Search on Thumbtack']"))
        )
        search_menu.clear()
        search_menu.click()
        human_typing(search_menu, niche)
        random_sleep()

        search_zip_code = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//input[@aria-label='Zip code']"))
        )

        search_zip_code.clear()
        for _ in range(10):
            sleep(.7)
            search_zip_code.send_keys(Keys.BACK_SPACE)

        human_typing(search_zip_code, zip_code)
        random_sleep()

        search_button = driver.find_element(By.XPATH, "//button[@data-test='search-button']")
        search_button.click()
        sleep(7)

        try:
            btn_close_popup = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "//section[contains(., 'Based on your search, we')]//button"))
            )
            btn_close_popup.click()
        except:
            pass
        sleep(5)

        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.XPATH, "//a[@data-testid='pro-list-result']"))
            )
        except Exception as e:
            print('Error in niche - ', niche, ' and zip code - ', zip_code, datetime.now(), str(e))
            return

        result_elements = driver.find_elements(By.XPATH, "//a[@data-testid='pro-list-result']")

        count = len(result_elements)
        print(f'Found {count} links in niche {niche} and zip code {zip_code}')

        list_hrefs = []
        for element in result_elements:
            href = element.get_attribute('href')
            list_hrefs.append(href)

        if list_hrefs:
            for k, link in enumerate(list_hrefs):
                # Rotate Proxy
                if k % 3 == 0:
                    proxy = await get_proxy()
                    proxy = {
                        'http': f'http://{proxy["username"]}:{proxy["password"]}@{proxy["ip"]}:{proxy["port"]}',
                        'https': f'https://{proxy["username"]}:{proxy["password"]}@{proxy["ip"]}:{proxy["port"]}',
                        'no_proxy': 'localhost,127.0.0.1'
                    }
                    driver.proxy = proxy

                for i in range(3):
                    try:
                        driver.get(link)
                        break
                    except:
                        sleep(2)

                if i == 2:
                    print(f'Error in {niche} and {zip_code} in link {link}')
                    continue

                random_sleep()

                # Check instagram
                links = driver.find_elements(By.XPATH, "//a[contains(@href, '/instagram/redirect')]")
                links_instagram = ", ".join([link.get_attribute('href') for link in links]) if links else None

                # Check facebook
                links = driver.find_elements(By.XPATH, "//a[contains(@href, '/facebook/redirect')]")
                links_facebook = ", ".join([link.get_attribute('href') for link in links]) if links else None

                # Title Company
                try:
                    h1_element = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "h1"))
                    )
                    h1_text = h1_element.text.strip()
                except:
                    h1_text = None

                # Extract credentials
                credentials_name = None
                credentials_licence = None
                try:
                    credentials_section = driver.find_element(By.ID, "ServicePageCredentialsSection")
                    text_content = credentials_section.text
                    split_text = text_content.split('\n')
                    split_text = [el.strip() for el in split_text if el.strip() != '']

                    if 'Background Check' in split_text:
                        credentials_name = split_text[split_text.index('Background Check') + 1].strip()

                    for item in split_text:
                        if 'License Type:' in item:
                            credentials_licence = item.replace('License Type:', '').strip()
                            break
                except:
                    pass

                row = {
                    'Niche': niche,
                    'Zip Code': zip_code,
                    'Name': h1_text,
                    'Instagram': links_instagram,
                    'Facebook': links_facebook,
                    'Credentials_Name': credentials_name,
                    'Credentials_Licence': credentials_licence,
                    'Email': None,  # Email extraction not implemented in the original code
                    'Phone': None   # Phone extraction not implemented in the original code
                }
                await save_to_csv(row, OUTPUT_FILE_PATH)

        await save_progress(niche, zip_code, PROGRESS_FILE_PATH)
    finally:
        driver.quit()


async def thumbtack_scraper(state, niche):


    print(datetime.now())


if __name__ == "__main__":
    asyncio.run(thumbtack_scraper())