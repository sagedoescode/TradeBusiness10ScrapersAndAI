import asyncio
import csv
import logging
import os
import random
import re

from playwright.async_api import async_playwright
from functions.useful import parse_proxy_file, get_verified_proxy,get_sleep_interval

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
async def random_sleep(min_seconds, max_seconds):
    await asyncio.sleep(random.uniform(min_seconds, max_seconds))


async def get_socials(business_info, proxy_path):
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

        try:
            await page.goto("https://www.google.com/", timeout=30000)
            search_input = page.get_by_label("Search", exact=True)
            for char in business_info:
                await search_input.type(char, delay=100)  # 100ms base delay
                await random_sleep(0.05, 0.2)  # Additional random delay between 50ms and 200ms

            await search_input.press("Enter")

            await page.wait_for_load_state("networkidle", timeout=30000)

            # Check if the search results are loaded
            if not await page.get_by_role("main").is_visible(timeout=5000):
                print("Search results not loaded. Trying another proxy.")
                return await get_socials(business_info, proxy_path)  # Recursive call with a new proxy

            social_links = {}
            for platform in ["Instagram", "LinkedIn", "Facebook"]:
                try:
                    # Try to find the link by exact name match
                    link = page.get_by_role("link", name=platform, exact=True)

                    # If Facebook link is not found or has multiple matches, try alternative selectors
                    if platform == "Facebook":
                        if await link.count() == 0:
                            # Try finding by partial text match
                            link = page.get_by_role("link", name=re.compile("facebook", re.IGNORECASE))

                        # If still not found or has multiple matches, try finding by URL pattern
                        if await link.count() == 0 or await link.count() > 1:
                            link = page.get_by_role("link", name=re.compile(r"facebook\.com"))

                        # If we still have multiple matches, select the first one with a valid Facebook URL
                        if await link.count() > 1:
                            all_links = await link.all()
                            for l in all_links:
                                href = await l.get_attribute("href")
                                if "facebook.com" in href:
                                    link = l
                                    break

                    if await link.count() > 0:
                        href = await link.get_attribute("href")
                        social_links[platform] = href
                    else:
                        social_links[platform] = ""
                except Exception as e:
                    print(f"Error scraping {platform} link: {str(e)}")
                    social_links[platform] = ""

            return social_links
        except Exception as e:
            print(f"An error occurred: {e}")
            return await get_socials(business_info, proxy_path)  # Recursive call with a new proxy

        finally:
            await context.close()
            await browser.close()

async def process_row(row, headers, proxy_path):
    if len(row) > 20 and row[20].lower() != 'n/a':
        business_name = row[2]
        address = row[20] if row[20] else row[21]  # Use city if address is empty
        business_info = f"{business_name}, {address}"

        # Check if we need to add social media data
        if not any(row[17:20]):  # Instagram 17, Facebook 18, LinkedIn 19
            try:
                await asyncio.sleep(get_sleep_interval("medium"))
                social_links = await get_socials(business_info, proxy_path)
                logging.info(f"Scraped links: {social_links}")

                # Update only the three columns for social media
                row[17] = social_links.get("Instagram", "")
                row[18] = social_links.get("Facebook", "")
                row[19] = social_links.get("LinkedIn", "")
            except Exception as e:
                logging.error(f"Error processing row: {e}")
        else:
            logging.info(f"Row already has social media data")
    else:
        logging.info(f"Skipping row: Invalid data")

    # Ensure the row has the same number of columns as the header
    while len(row) < len(headers):
        row.append("")

    return row


async def process_csv(state, niche, max_workers):
    PROXY_FILE_PATH = r'C:\Users\Sage\PycharmProjects\MasterScraper/proxies/proxies.txt'
    INPUT_CSV_PATH = fr"C:\Users\Sage\PycharmProjects\MasterScraper\data\{state}_{niche}_central_stage_2.csv"
    OUTPUT_CSV_PATH = rf'C:\Users\Sage\PycharmProjects\MasterScraper/data/updated_{state}_{niche}_businesses_with_socials.csv'
    PROGRESS_FILE = rf'C:\Users\Sage\PycharmProjects\MasterScraper/data/progress_{state}_{niche}_search.csv'

    logging.info(f"Starting process for state: {state}, niche: {niche}")

    if not os.path.exists(INPUT_CSV_PATH):
        logging.error(f"Input CSV file not found: {INPUT_CSV_PATH}")
        return

    # Read the progress file to get processed rows
    processed_rows = set()
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            processed_rows = set(int(row[0]) for row in reader)
        logging.info(f"Resuming from {len(processed_rows)} processed rows")
    else:
        logging.info("Starting from the beginning")

    # Dictionary to cache scraped data for each unique business
    business_cache = {}

    async def worker(queue, writer, headers):
        while True:
            row_num, row = await queue.get()
            if row is None:
                queue.task_done()
                break

            business_name = row[2]
            if business_name in business_cache:
                # Use cached data for duplicate businesses
                social_links = business_cache[business_name]
                logging.info(f"Using cached data for: {business_name}")
            else:
                # Scrape new data for unique businesses
                new_row = await process_row(row, headers, PROXY_FILE_PATH)
                social_links = {
                    "Instagram": new_row[17],
                    "Facebook": new_row[18],
                    "LinkedIn": new_row[19]
                }
                business_cache[business_name] = social_links

            # Update the row with social media data
            row[17] = social_links["Instagram"]
            row[18] = social_links["Facebook"]
            row[19] = social_links["LinkedIn"]

            writer.writerow(row)
            with open(PROGRESS_FILE, 'a', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow([row_num])
            queue.task_done()

    try:
        mode = 'a' if os.path.exists(OUTPUT_CSV_PATH) else 'w'

        # Open the input file for reading
        with open(INPUT_CSV_PATH, 'r', newline='', encoding='utf-8') as input_file:
            reader = csv.reader(input_file)
            headers = next(reader)

            # Open the output file for writing (or appending)
            with open(OUTPUT_CSV_PATH, mode, newline='', encoding='utf-8') as output_file:
                writer = csv.writer(output_file)

                if mode == 'w':
                    writer.writerow(headers)

                queue = asyncio.Queue()
                for row_num, row in enumerate(reader, start=1):
                    if row_num not in processed_rows:
                        await queue.put((row_num, row))

                workers = [asyncio.create_task(worker(queue, writer, headers)) for _ in range(max_workers)]

                await queue.join()

                for _ in range(max_workers):
                    await queue.put((None, None))

                await asyncio.gather(*workers)

        logging.info("Process completed successfully")
    except Exception as e:
        logging.error(f"An error occurred during processing: {e}")
    finally:
        logging.info(f"Progress saved in {PROGRESS_FILE}")

async def social_google_search(state, niche, max_workers):
    await process_csv(state, niche, max_workers)



async def main():
    state = "Massachusetts"
    niche = "Marble & Granite"
    max_workers = 15  # You can adjust this number based on your system's capabilities

    await social_google_search(state, niche, max_workers)

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
