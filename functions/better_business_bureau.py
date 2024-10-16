import random
import re
import asyncio
import csv
import json
from typing import List, Dict, Tuple
from playwright.async_api import async_playwright, Playwright, Page, TimeoutError as PlaywrightTimeoutError, expect

from functions.useful import verify_proxy, get_random_proxy, parse_proxy_file, get_state_abbreviations

async def bbb_scraper(niche, state):
    print("WORKING")
    PROXY_FILE_PATH = r'C:\Users\Sage\PycharmProjects\MasterScraper/proxies/proxies.txt'
    CSV_FILE_PATH = r'C:\Users\Sage\PycharmProjects\MasterScraper\data\geodata\all_us_cities.csv'
    PROGRESS_FILE_PATH = r'C:\Users\Sage\PycharmProjects\MasterScraper\data\scraping_progress_bbb.json'
    OUTPUT_CSV_PATH = fr'C:\Users\Sage\PycharmProjects\MasterScraper\data\scraped_data_bbb_{niche}_{state}.csv'
    MAX_WORKERS = 15

    async def get_verified_proxy(proxies):
        while True:
            proxy = get_random_proxy(proxies)
            if await verify_proxy(proxy):
                return proxy

    async def launch_browser(playwright: Playwright, proxy: dict, headless: bool = False):
        browser_args = {
            'headless': headless,
            'proxy': {
                "server": f"{proxy['ip']}:{proxy['port']}",
                "username": proxy['username'],
                "password": proxy['password']
            }
        }
        browser = await playwright.chromium.launch(**browser_args)
        context = await browser.new_context(viewport={'width': 1920, 'height': 1000})
        page = await context.new_page()
        return browser, context, page

    async def delay():
        await asyncio.sleep(random.uniform(0.5, 1))


    async def click_with_interception_handling(page, link_locator, max_attempts, url):
        for attempt in range(max_attempts):
            try:
                await page.wait_for_load_state('domcontentloaded')

                # Wait for the link to be visible, enabled, and stable
                await expect(link_locator).to_be_visible()
                await expect(link_locator).to_be_enabled()

                # Try to click the link
                await link_locator.click(timeout=30000)
                await page.wait_for_load_state('domcontentloaded')

                # If click succeeds, break the loop
                print(f"Attempt {attempt + 1}: Link clicked successfully.")
                break


            except Exception as e:
                print(f"Attempt {attempt + 1} failed")

                content = await page.content()
                if "took too long to respond" in str(content):
                    print("Site can't be reached. Waiting and reloading...")
                    await page.wait_for_timeout(4000)
                    await page.reload()
                    await page.wait_for_timeout(4000)

                intercepting_div = page.locator('div.text-white.css-aczujz.ez77h630')
                intercepting_div_2 = page.locator('section.css-1kzsvch.e5ukq110')

                if await intercepting_div_2.is_visible():
                    print(f"Intercepting div 2 found. Going back.")
                    await page.goto(url)
                    await page.wait_for_timeout(3000)  # Replace multiple delay() calls
                    await page.wait_for_load_state('domcontentloaded')
                elif await intercepting_div.is_visible():
                    print(f"Intercepting div 1 found. Waiting...")
                    await page.wait_for_timeout(4000)  # Replace multiple delay() calls
                    await page.wait_for_load_state('domcontentloaded')

                # If it's the last attempt, raise the exception
                if attempt == max_attempts - 1:
                    print(content)

                    raise Exception(f"Failed to click link and handle interceptors after {max_attempts} attempts")

            # Add a small delay between attempts
            await page.wait_for_timeout(10000)

    async def extract_business_details(page, max_attempts, url):
        details = {}

        try:
            # Click "Read More Business Details and See Alerts" link
            read_more_locator = page.locator('a.dtm-read-more').first
            await click_with_interception_handling(page, read_more_locator, max_attempts, url)
            await delay()

            # Extract business name
            business_name_locator = page.locator('h1.bds-h3')
            full_text = await business_name_locator.inner_text()
            details['business_name'] = full_text.replace("Additional Information for", "").strip()

            # Extract specific details using more precise selectors
            async def get_detail(selector, key):
                try:
                    elements = await page.query_selector_all(selector)
                    if len(elements) > 0:
                        return await elements[0].inner_text()
                except Exception as e:
                    print(f"Error extracting {key}: {e}")
                return ""

            details['Business Management'] = await get_detail('dt:has-text("Business Management") + dd',
                                                              'Business Management')
            details['Business Started'] = await get_detail('dt:has-text("Business Started:") + dd', 'Business Started')
            details['Business Incorporated'] = await get_detail('dt:has-text("Business Incorporated:") + dd',
                                                                'Business Incorporated')
            details['Type of Entity'] = await get_detail('dt:has-text("Type of Entity") + dd', 'Type of Entity')
            details['Number of Employees'] = await get_detail('dt:has-text("Number of Employees") + dd',
                                                              'Number of Employees')
            details['Business Categories'] = await get_detail('dt:has-text("Business Categories") + dd',
                                                              'Business Categories')

            # Extract hours of operation
            hours_locator = page.locator('dt:has-text("Hours of Operation") ~ dd')
            hours_count = await hours_locator.count()
            hours = []
            for i in range(hours_count):
                hour_text = await hours_locator.nth(i).inner_text()
                hours.append(hour_text.strip())
            details['Hours of Operation'] = ', '.join(hours)

            # Rest of your existing code for extracting other details
            management_info = details.get('Business Management', '')
            details['management'] = \
            management_info.replace("Mr. ", "").replace("Mrs. ", "").replace("Ms. ", "").split(",")[0]

            # Extract phone numbers
            phone_numbers = []
            business_phone_locator = page.locator('div.with-icon a.dtm-phone').first
            business_phone = await business_phone_locator.inner_text()

            other_phone_locators = page.locator('li:has(a.dtm-phone):has-text("Other Phone")')
            other_phone_count = await other_phone_locators.count()

            for i in range(other_phone_count):
                other_phone = await other_phone_locators.nth(i).locator('a.dtm-phone').inner_text()
                phone_numbers.append(other_phone)

            details['business_phone'] = business_phone
            details['management_phone'] = '/'.join(phone_numbers)

            address_locator = page.locator('div.dtm-address dd')
            address_text = await address_locator.inner_text()
            details['business_address'] = address_text.split('\n')[0].strip()

            # Extract first category
            categories = details.get('Business Categories', '').split(',')
            details['first_category'] = categories[0].strip() if categories else ''

            # Extract first website
            website_locator = page.locator('a.dtm-url').first
            details['website'] = await website_locator.get_attribute(
                'href') if await website_locator.count() > 0 else ''

            # Extract social media (Facebook, Instagram, or LinkedIn)
            social_media = ''
            facebook_locator = page.locator('a[href*="facebook.com"]').first
            instagram_locator = page.locator('a[href*="instagram.com"]').first
            linkedin_locator = page.locator('a[href*="linkedin.com"]').first

            if await facebook_locator.count() > 0:
                social_media = await facebook_locator.get_attribute('href')
            elif await instagram_locator.count() > 0:
                social_media = await instagram_locator.get_attribute('href')
            elif await linkedin_locator.count() > 0:
                social_media = await linkedin_locator.get_attribute('href')

            details['social_media'] = social_media

        except Exception as e:
            print(f"Error in extract_business_details: {e}")

        print(details)
        return details

    async def search_contractors(page: Page, search_term: str, location: str, csv_file, progress):
        city, state = location.split(', ')
        niche = search_term.replace(' contractors', '')

        if progress.get(state, {}).get(city, {}).get(niche, {}).get('completed', False):
            print(f"Skipping {niche} in {city}, {state} - already completed")
            return

        current_page = progress.get(state, {}).get(city, {}).get(niche, {}).get('page', 1)
        await page.goto("https://www.bbb.org/", wait_until="domcontentloaded")

        await delay()
        await delay()
        await delay()
        await delay()
        await delay()
        await delay()
        await delay()
        await delay()
        await delay()
        await delay()
        await delay()
        await delay()
        await page.wait_for_load_state('domcontentloaded')
        async def minimize():
            try:
                minimize_button = await page.query_selector('button[aria-label="Minimize window"]')
                if minimize_button:
                    await minimize_button.click()
                    await delay()
                else:
                    pass
            except:
                pass

        await minimize()
        await page.get_by_role("combobox", name="Find").click()
        await delay()
        await minimize()
        await page.get_by_role("combobox", name="Find").fill(search_term)
        await delay()
        await minimize()
        await page.get_by_role("combobox", name="Find").click()
        await minimize()
        await delay()
        await page.get_by_role("combobox", name="Find").press("ArrowDown")
        await minimize()
        await delay()
        await page.get_by_role("combobox", name="Find").press("Enter")
        await minimize()
        await delay()
        await minimize()
        await page.get_by_role("combobox", name="Near").click()
        await page.get_by_role("combobox", name="Near").click()
        await page.get_by_role("combobox", name="Near").click()
        await delay()
        await minimize()
        await page.get_by_role("combobox", name="Near").fill(location)
        await delay()
        await minimize()

        await page.get_by_role("button", name="Search", exact=True).click()
        await page.wait_for_load_state('domcontentloaded')
        await delay()
        await delay()
        await delay()
        await delay()
        await delay()
        await delay()
        await delay()
        await delay()
        await page.click('input#sa-input')
        await delay()
        await delay()
        await delay()
        await delay()
        await page.wait_for_load_state('domcontentloaded')
        current_page = progress.get(state, {}).get(city, {}).get(niche, {}).get('page', 1)
        page_number = current_page

        while True:
            print(f"Processing page {page_number} for {niche} in {city}, {state}")

            result_links_count = await page.locator('.result-business-name a').count()

            for i in range(result_links_count):
                url = page.url
                try:
                    # Use nth() to get each link
                    link_locator = page.locator('.result-business-name a').nth(i)
                    href = await link_locator.get_attribute('href')
                    print(f"Link {i}: {href}")

                    if href and "ad" in href or "adclick" in href:

                        continue

                    await delay()
                    await delay()
                    await delay()
                    await delay()

                    await click_with_interception_handling(page, link_locator, max_attempts=6, url=url)
                    await delay()
                    await delay()
                    await delay()
                    details = await extract_business_details(page, max_attempts=6, url=url)

                    # Write details to CSV
                    csv_writer = csv.writer(csv_file)
                    csv_writer.writerow([
                        city,
                        state,
                        niche,
                        details.get('management', ''),
                        details.get('business_phone', ''),
                        details.get('management_phone', ''),
                        details.get('business_address', ''),
                        details.get('first_category', ''),
                        details.get('website', ''),
                        details.get('business_name', ''),
                        details.get('social_media', '')
                    ])
                    # Flush the file buffer to ensure data is written
                    csv_file.flush()

                    # Update progress
                    if state not in progress:
                        progress[state] = {}
                    if city not in progress[state]:
                        progress[state][city] = {}
                    if niche not in progress[state][city]:
                        progress[state][city][niche] = {}
                    progress[state][city][niche]['page'] = page_number

                    # Save progress to file
                    with open(PROGRESS_FILE_PATH, 'w') as f:
                        json.dump(progress, f)

                    await page.go_back()

                    await page.wait_for_load_state('domcontentloaded')
                    await delay()
                    await delay()
                    await page.go_back()

                    await page.wait_for_load_state('domcontentloaded')
                    await delay()
                    await delay()
                    await delay()
                    print(f"Processed result {i + 1} on page {page_number}")
                except Exception as e:
                    print(f"Error processing result {i + 1} on page {page_number}: {e}")
                    continue

            # Check if there's a next page
            await delay()
            await delay()
            await delay()
            await delay()
            next_button = await page.query_selector('a[rel="next"]')
            if not next_button:
                print(f"No more pages to process for {niche} in {city}, {state}")
                progress[state][city][niche]['completed'] = True
                with open(PROGRESS_FILE_PATH, 'w') as f:
                    json.dump(progress, f)
                break

            await next_button.click()
            await delay()
            page_number += 1

        print(f"Search completed for {niche} in {city}, {state}")


    def read_cities_from_csv(state: str) -> List[str]:
        cities = []
        with open(CSV_FILE_PATH, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['STATE_NAME'] == state:

                    cities.append(row['CITY'])
        return cities


    async def worker(queue: asyncio.Queue, all_proxies: List[Dict], csv_file, progress):
        while True:
            niche, city, state = await queue.get()
            proxy = await get_verified_proxy(all_proxies)
            print(f"Worker processing {city} for {niche} using proxy: {proxy['ip']}:{proxy['port']}")
            state_abbreviations = get_state_abbreviations()
            state_abbr = state_abbreviations.get(state.lower())
            location = f"{city}, {state_abbr}"

            async with async_playwright() as playwright:
                browser, context, page = await launch_browser(playwright, proxy, headless=False)

                try:
                    await search_contractors(page, f"{niche} contractors", location, csv_file, progress)
                except Exception as e:
                    print(f"An error occurred while processing {city} for {niche}: {e}")
                finally:
                    await context.close()
                    await browser.close()

            queue.task_done()

    async def process_niches(niche:str, state: str):
        cities = read_cities_from_csv(state)
        tasks = []

        for city in cities:
            tasks.append((niche, city, state))
        return tasks


    async def main(niche: str, state: str):
        all_proxies = parse_proxy_file(PROXY_FILE_PATH)
        tasks = await process_niches(niche, state)

        # Load existing progress
        try:
            with open(PROGRESS_FILE_PATH, 'r') as f:
                progress = json.load(f)
        except FileNotFoundError:
            progress = {}

        with open(OUTPUT_CSV_PATH, 'a', newline='', encoding='utf-8') as csv_file:
            # Write header only if the file is empty
            if csv_file.tell() == 0:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow([
                    'City', 'State', 'Niche', 'Name', 'Business Phone', 'Management Phone',
                    'Business Address', 'Business Category', 'Website', 'Business Name', 'Social Media'
                ])
                csv_file.flush()

            queue = asyncio.Queue()
            for task in tasks:
                await queue.put(task)

            workers = [asyncio.create_task(worker(queue, all_proxies, csv_file, progress)) for _ in
                       range(min(MAX_WORKERS, len(tasks)))]

            await queue.join()

            for w in workers:
                w.cancel()

            await asyncio.gather(*workers, return_exceptions=True)

    await main(niche, state)


async def run_bbb_scraper(niche: str, state: str):
    try:
        await bbb_scraper(niche, state)
    except KeyboardInterrupt:
        print("Script stopped by user. Progress has been saved.")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")


if __name__ == "__main__":
    niche = "Roofing"
    state = "Massachusetts"
    asyncio.run(run_bbb_scraper(niche="Marble & Granite", state="Massachusetts"))