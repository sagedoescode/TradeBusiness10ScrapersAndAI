import random
from playwright.async_api import async_playwright
import asyncio
import re
from functions.useful import verify_proxy, get_random_proxy, parse_proxy_file
import csv
import os
from asyncio import Queue, Semaphore


async def run_scraper(state, niche):
    PROXY_FILE_PATH = r'C:\Users\Sage\PycharmProjects\MasterScraper/proxies/proxies.txt'
    INPUT_CSV_PATH = fr'C:\Users\Sage\PycharmProjects\MasterScraper\data\{state}_{niche}_central_stage_4.csv'
    OUTPUT_CSV_PATH = fr'C:\Users\Sage\PycharmProjects\MasterScraper\data/scraped_phone_numbers_{state}_{niche}.csv'
    PROGRESS_FILE_PATH = fr'C:\Users\Sage\PycharmProjects\MasterScraper\data\scraping_progress_{state}_{niche}_phone.txt'
    NUM_WORKERS = 20

    def load_input_csv():
        with open(INPUT_CSV_PATH, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            return [row for row in reader]

    def save_to_csv(data, filename=OUTPUT_CSV_PATH):
        fieldnames = ['Category', 'Language', 'Business Name', 'Phone #', 'Phone # 2', 'Website', 'Site Rating',
                      'Reviews', 'Rating', 'Owner First Name', 'Owner Last Name', 'Owners Cel #', 'Owners Phone #',
                      'Personal Email', 'Business Email', 'Owner Social Media', 'Owner Social Media 2', 'Instagram',
                      'Facebook', 'Linkedin', 'Business Address', 'City', 'County', 'State', 'Google Link', 'Plus Code',
                      'Source']

        with open(filename, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if file.tell() == 0:
                writer.writeheader()
            for row in data:
                writer.writerow(row)

        print(f"Data appended to {filename}")

    def load_progress():
        if os.path.exists(PROGRESS_FILE_PATH):
            with open(PROGRESS_FILE_PATH, 'r', encoding='utf-8') as file:
                return set(file.read().splitlines())
        return set()

    def save_progress(name):
        with open(PROGRESS_FILE_PATH, 'a', encoding='utf-8') as file:
            file.write(f"{name}\n")

    async def random_sleep(min_seconds=1, max_seconds=5):
        sleep_time = random.uniform(min_seconds, max_seconds)
        await asyncio.sleep(sleep_time)

    async def get_verified_proxy(proxies):
        while True:
            proxy = get_random_proxy(proxies)
            if await verify_proxy(proxy):
                return proxy

    async def process_match(browser, result, original_row):
        context = await browser.new_context()
        page = await context.new_page()

        await random_sleep()
        await page.goto("https://www.anywho.com/")
        await random_sleep()
        await page.wait_for_load_state("networkidle")
        try:
            close_button = await page.wait_for_selector('#onetrust-close-btn-container button', timeout=5000)
            if close_button:
                await random_sleep()
                await close_button.click()
        except:
            print("Cookie banner not found or already dismissed")
        try:
            await random_sleep()
            await page.locator("#checkbox").click()
        except:
            print("Checkbox not found or already dismissed")
        await random_sleep()

        person_info = await result.query_selector(".person-info")
        if person_info:
            await random_sleep()
            info_text = await person_info.inner_text()

            name_age_match = re.search(r'(\w+(?:\s+\w+)*),\s*Age\s+(\d+)', info_text)
            full_name = name_age_match.group(1) if name_age_match else "Unknown"
            age = name_age_match.group(2) if name_age_match else "Unknown"

            address_match = re.search(r'(.*?),\s*(\w{2})\s+(\d{5}(?:-\d{4})?)', info_text)
            address = address_match.group(0) if address_match else "Unknown"

            city_state_match = re.search(r'([\w\s]+),\s*(\w{2})', address)
            city = city_state_match.group(1) if city_state_match else "Unknown"
            state = city_state_match.group(2) if city_state_match else "Unknown"
            details = []
            if name_age_match:
                details.append(f"{full_name}, Age {age}")
            if address_match:
                details.append(address)
            if city_state_match:
                details.append(f"{city}, {state}")

            details_str = ", ".join(details) if details else "No details available"
            await random_sleep()
            view_profile = await result.query_selector(".view-profile")
            if view_profile:
                await random_sleep()
                profile_url = await view_profile.get_attribute("href")
                await random_sleep()
                await page.goto(f"https://www.anywho.com{profile_url}")
                await random_sleep()
                await page.wait_for_load_state("networkidle")

                phone_link = await page.query_selector("a[href^='/phone/']")
                if phone_link:
                    await random_sleep()
                    await phone_link.click()

                await random_sleep()
                await page.wait_for_load_state("networkidle")

                phone_element = await page.query_selector(".phone-number")
                if phone_element:
                    await random_sleep()
                    phone_number = await phone_element.inner_text()
                    data = {
                        'Category': '',
                        'Language': '',
                        'Business Name': '',
                        'Phone #': '',
                        'Phone # 2': '',
                        'Website': '',
                        'Site Rating': '',
                        'Reviews': '',
                        'Rating': '',
                        'Owner First Name': full_name.split()[0] if full_name != "Unknown" else '',
                        'Owner Last Name': ' '.join(full_name.split()[1:]) if full_name != "Unknown" else '',
                        'Owners Cel #': details_str,
                        'Owners Phone #': phone_number,
                        'Personal Email': '',
                        'Business Email': '',
                        'Owner Social Media': '',
                        'Owner Social Media 2': '',
                        'Instagram': '',
                        'Facebook': '',
                        'Linkedin': '',
                        'Business Address': address,
                        'City': city,
                        'County': '',
                        'State': state,
                        'Google Link': '',
                        'Plus Code': '',
                        'Source': 'Anywho'
                    }

                    # If original_row is a dictionary, update data with its values
                    if isinstance(original_row, dict):
                        for key in data.keys():
                            if key in original_row and original_row[key]:
                                data[key] = original_row[key]
                    elif isinstance(original_row, str):
                        # If original_row is a string (assumed to be the phone number), update Owners Phone #
                        data['Owners Phone #'] = original_row

                    return data
                else:
                    print(f"Phone number not found for {full_name}")
                    return None
            else:
                print(f"Profile link not found for {full_name}")
        else:
            print("Person info not found")

        await random_sleep()
        await context.close()

    async def create_name_variations(first_name, last_name):
        variations = [
            f"{first_name} {last_name}",  # Full name as provided
            f"{first_name} {last_name.replace('.', '')}",  # Remove any dots
        ]

        # Handle potential middle initial
        name_parts = last_name.split()
        if len(name_parts) > 1:
            variations.append(f"{first_name} {name_parts[-1]}")  # First name + last part of last name
            if len(name_parts[0]) == 1 or (len(name_parts[0]) == 2 and name_parts[0].endswith('.')):
                # If first part looks like an initial, add variation without it
                variations.append(f"{first_name} {' '.join(name_parts[1:])}")

        return list(set(variations))


    async def worker(worker_id, task_queue, all_proxies, playwright, result_queue, name_locks, scraped_names):
        print(f"Worker {worker_id} started")
        while True:
            try:
                row = await task_queue.get()
                if row is None:
                    print(f"Worker {worker_id} finished")
                    break

                first_name = row['Owner First Name'].strip()
                last_name = row['Owner Last Name'].strip()

                if not first_name and not last_name:
                    print(f"Worker {worker_id}: Skipping entry with empty owner name")
                    continue

                name_variations = await create_name_variations(first_name, last_name)

                for full_name in name_variations:
                    if full_name in scraped_names:
                        print(f"Worker {worker_id}: Skipping already processed name: {full_name}")
                        continue

                    async with name_locks[full_name]:
                        if full_name in scraped_names:
                            print(f"Worker {worker_id}: Name was processed by another worker: {full_name}")
                            continue

                        # Mark the name as being processed immediately
                        scraped_names.add(full_name)

                        try:
                            proxy = await get_verified_proxy(all_proxies)
                            browser_args = {
                                'headless': False,
                                'proxy': {
                                    "server": f"{proxy['ip']}:{proxy['port']}",
                                    "username": proxy['username'],
                                    "password": proxy['password']
                                }
                            }

                            async with await playwright.chromium.launch(**browser_args) as browser:
                                context = await browser.new_context()
                                page = await context.new_page()

                                state = "MA"

                                await random_sleep()
                                await page.goto("https://www.anywho.com/")
                                await random_sleep()
                                await page.wait_for_load_state("networkidle")
                                try:
                                    close_button = await page.wait_for_selector('#onetrust-close-btn-container button',
                                                                                timeout=5000)
                                    if close_button:
                                        await random_sleep()
                                        await close_button.click()
                                except:
                                    print("Cookie banner not found or already dismissed")
                                try:
                                    await random_sleep()
                                    await page.locator("#checkbox").click()
                                except:
                                    print("Checkbox not found or already dismissed")
                                await random_sleep()
                                await page.screenshot(path=f'screenshot_{worker_id}.png', full_page=True)
                                await random_sleep()
                                await page.get_by_placeholder("e.g. John").fill(first_name)
                                await random_sleep()
                                await page.get_by_placeholder("e.g. Smith").fill(last_name)
                                await random_sleep()
                                await page.get_by_label("State").select_option(state)
                                await random_sleep()
                                await page.get_by_role("button", name="Find").click()

                                await random_sleep()
                                await page.wait_for_load_state("networkidle")

                                results = await page.query_selector_all(".person.result-item")
                                matches = []
                                for result in results:
                                    await random_sleep()
                                    result_info = await result.query_selector(".person-info")
                                    if result_info:
                                        await random_sleep()
                                        info_text = await result_info.inner_text()
                                        if state in info_text:
                                            matches.append(result)

                                if matches:
                                    print(f"Worker {worker_id}: Found {len(matches)} matches for {full_name} in MA:")
                                    tasks = [process_match(browser, result, row) for result in matches]
                                    results = await asyncio.gather(*tasks)
                                    valid_results = [result for result in results if result]
                                    if valid_results:
                                        await result_queue.put(valid_results)
                                        scraped_names.add(full_name)
                                else:
                                    print(f"Worker {worker_id}: No matches found for {full_name} in MA")
                                    scraped_names.add(full_name)

                                await context.close()
                        except Exception as e:
                            print(f"Worker {worker_id} Error processing {full_name}: {e}")

            except Exception as e:
                print(f"Worker {worker_id} Unexpected error: {e}")
            finally:
                task_queue.task_done()

    async def result_writer(result_queue, scraped_names):
        while True:
            results = await result_queue.get()
            if results is None:
                break
            if results:  # Only process non-empty result lists
                save_to_csv(results)
                full_name = f"{results[0]['Owner First Name']} {results[0]['Owner Last Name']}"
                save_progress(full_name)

            result_queue.task_done()


    async def run(playwright):
        all_proxies = parse_proxy_file(PROXY_FILE_PATH)
        input_data = load_input_csv()
        scraped_names = set(load_progress())

        task_queue = Queue()
        result_queue = Queue()

        name_locks = {f"{row['Owner First Name'].strip()} {row['Owner Last Name'].strip()}": asyncio.Lock()
                      for row in input_data}

        for row in input_data:
            full_name = f"{row['Owner First Name'].strip()} {row['Owner Last Name'].strip()}"
            if full_name not in scraped_names:
                await task_queue.put(row)

        workers = [
            asyncio.create_task(worker(i, task_queue, all_proxies, playwright, result_queue, name_locks, scraped_names))
            for i in range(NUM_WORKERS)]

        writer_task = asyncio.create_task(result_writer(result_queue, scraped_names))

        await task_queue.join()

        for _ in range(NUM_WORKERS):
            await task_queue.put(None)

        await asyncio.gather(*workers)
        await result_queue.put(None)
        await writer_task


    async with async_playwright() as playwright:
        await run(playwright)
# This function can be imported and called from another script
async def main_scraper_anywho(state, niche):

    await run_scraper(state, niche)
#
#asyncio.run(main_scraper_anywho(state="Massachusetts", niche="Counter Top"))
