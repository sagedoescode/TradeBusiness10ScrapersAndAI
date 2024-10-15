import random
import asyncio
import csv
from playwright.async_api import async_playwright, TimeoutError
import os
import aiohttp
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

def get_random_proxy(proxies):
    return random.choice(proxies)

def get_sleep_interval(interval_type):
    intervals = {
        "tiny": random.uniform(0.5, 1.5),
        "short": random.uniform(2.5, 3.5),
        "medium": random.uniform(3, 5),
        "long": random.uniform(5, 8),
        "infinite": float('inf'),
    }
    return intervals.get(interval_type, 2)

async def extract_owner_names(page):
    owners = []
    rows = await page.query_selector_all('table#MainContent_grdOfficers tr.GridRow')
    for row in rows:
        name = await row.query_selector('td:nth-child(2)')
        if name:
            name_text = await name.inner_text()
            name_parts = name_text.strip().split()
            if len(name_parts) >= 2:
                owners.append((name_parts[0], ' '.join(name_parts[1:])))
    return owners

async def scrape_business(page, business_name):
    try:
        await page.goto('https://corp.sec.state.ma.us/corpweb/CorpSearch/CorpSearch.aspx')
        await page.fill('input#MainContent_txtEntityName', business_name)
        await page.click('input#MainContent_btnSearch')
        await page.wait_for_selector('table#MainContent_SearchControl_grdSearchResultsEntity', timeout=10000)
        await page.click('table#MainContent_SearchControl_grdSearchResultsEntity a.link')
        await page.wait_for_selector('table#MainContent_grdOfficers', timeout=10000)
        owners = await extract_owner_names(page)
        return owners
    except Exception as e:
        print(f"Error scraping {business_name}: {e}")
        return []

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

async def process_batch(playwright, batch, CSV_FILE_PATH, PROXY_FILE_PATH, OUTPUT_CSV_PATH):
    all_proxies = parse_proxy_file(PROXY_FILE_PATH)
    proxy = await get_verified_proxy(all_proxies)
    print(f"Worker using Proxy: {proxy}")

    browser_args = {
        'headless': False,
        'proxy': {
            "server": f"{proxy['ip']}:{proxy['port']}",
            "username": proxy['username'],
            "password": proxy['password']
        }
    }

    browser = await playwright.firefox.launch(**browser_args)
    context = await browser.new_context(viewport={"width": 1280, "height": 800})
    page = await context.new_page()

    processed_businesses = {}
    if os.path.exists(OUTPUT_CSV_PATH) and os.path.getsize(OUTPUT_CSV_PATH) > 0:
        with open(OUTPUT_CSV_PATH, 'r', newline='') as outfile:
            reader = csv.reader(outfile)
            next(reader)
            for row in reader:
                if row[2]:
                    processed_businesses[row[2]] = row

    for row in batch:
        business_name = row[2]

        if business_name in processed_businesses:
            print(f"Skipping {business_name} - already processed")
            continue
        print(business_name)
        try:
            owners = await scrape_business(page, business_name)
        except:
            owners = []

        async with asyncio.Lock():
            with open(OUTPUT_CSV_PATH, 'a', newline='') as outfile:
                writer = csv.writer(outfile)
                if not owners:
                    writer.writerow(row + ['', ''])
                else:
                    unique_owners = set(owners)  # Remove duplicates
                    for first_name, last_name in unique_owners:
                        new_row = row + [first_name, last_name]
                        writer.writerow(new_row)

        print(f"Processed {business_name}")
        await asyncio.sleep(get_sleep_interval("short"))

    await browser.close()

async def ma_sec(niche, state):
    CSV_FILE_PATH = rf'C:\Users\Sage\PycharmProjects\MasterScraper/data/{state}_{niche}_central_stage_1.csv'
    PROXY_FILE_PATH = r'C:\Users\Sage\PycharmProjects\MasterScraper/proxies/proxies.txt'
    OUTPUT_CSV_PATH = rf'C:\Users\Sage\PycharmProjects\MasterScraper/data/updated_ma_sec_{state}_{niche}_businesses.csv'

    # Read input CSV and filter unique businesses
    unique_businesses = set()
    unique_rows = []
    with open(CSV_FILE_PATH, 'r', newline='') as infile:
        reader = csv.reader(infile)
        headers = next(reader)
        for row in reader:
            business_name = row[2]
            if business_name not in unique_businesses:
                unique_businesses.add(business_name)
                unique_rows.append(row)

    print(f"Total businesses: {len(unique_rows)}")
    print(f"Unique businesses: {len(unique_businesses)}")

    if not os.path.exists(OUTPUT_CSV_PATH) or os.path.getsize(OUTPUT_CSV_PATH) == 0:
        with open(OUTPUT_CSV_PATH, 'w', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(headers + ['Owner First Name', 'Owner Last Name'])

    num_workers = 10
    batch_size = len(unique_rows) // num_workers
    batches = [unique_rows[i:i + batch_size] for i in range(0, len(unique_rows), batch_size)]

    async with async_playwright() as playwright:
        tasks = [process_batch(playwright, batch, CSV_FILE_PATH, PROXY_FILE_PATH, OUTPUT_CSV_PATH) for batch in batches]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(ma_sec(niche="Marble & Granite", state="MAssachusetts"))
