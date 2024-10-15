import asyncio
import csv
import os
import re
import math
import random
from concurrent.futures import ProcessPoolExecutor, as_completed

import aiohttp
from playwright.async_api import async_playwright
from functions.useful import clean_text, get_sleep_interval, get_random_proxy, parse_proxy_file, get_state_abbreviations

MAX_WORKERS = 10
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



def is_address_in_state(address, state):
    state_abbreviations = get_state_abbreviations()
    state_abbr = state_abbreviations.get(state.lower())
    if not state_abbr:
        return False
    return bool(re.search(rf'\b{state_abbr}\b', address))

async def get_verified_proxy(proxies):
    while True:
        proxy = random.choice(proxies)
        if await verify_proxy(proxy):
            return proxy
        print(f"Proxy {proxy['ip']}:{proxy['port']} failed verification, trying another...")
def save_to_csv(businesses, filename, mode, state):
    if not businesses:
        print("No data to save.")
        return

    fieldnames = businesses[0].keys()
    temp_filename = f"{filename}.temp"

    try:
        with open(temp_filename, mode=mode, newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if mode == 'w':
                writer.writeheader()
            for business in businesses:
                if is_address_in_state(business['address'], state):
                    cleaned_business = {k: clean_text(str(v)) for k, v in business.items()}
                    writer.writerow(cleaned_business)

        # If writing was successful, rename the temp file to the actual filename
        os.replace(temp_filename, filename)
        print(f"Data successfully saved to {filename}")
    except Exception as e:
        print(f"Error saving data to {filename}: {e}")
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

def calculate_steps(zoom_level):
    meters_per_pixel = 11500  # Meters
    return meters_per_pixel

def generate_coordinates(top_left, bottom_right, step):
    coordinates = []
    lat_step = step / 111320
    lng_step = step / (111320 * math.cos(math.radians((top_left['lat'] + bottom_right['lat']) / 2)))
    lat = top_left['lat']
    direction = 1
    while lat >= bottom_right['lat']:
        if direction == 1:
            lng = top_left['lng']
            while lng <= bottom_right['lng']:
                coordinates.append((lat, lng))
                lng += lng_step
        else:
            lng = bottom_right['lng']
            while lng >= top_left['lng']:
                coordinates.append((lat, lng))
                lng -= lng_step
        lat -= lat_step
        direction *= -1
    return coordinates

async def get_business_details(page):
    selectors = {
        "name": "h1.DUwDvf",
        "rating": "span.ceNzKf[role='img']",
        "reviews_count": "span[aria-label$='reviews']",
        "category": "button.DkEaL",
        "address": "button[data-item-id='address']",
        "location": "button:has-text('Located in:')",
        "hours": "button[data-item-id='oh']:has-text('Open')",
        "website": "a[data-item-id='authority']",
        "plus_code": "button[data-item-id='oloc']",
        "phone": "button[data-item-id^='phone:tel:']"
    }

    business_details = {}
    for key, selector in selectors.items():
        try:
            element = page.locator(selector).first
            if await element.count() > 0:
                if key in ["phone", "plus_code"]:
                    aria_label = await element.get_attribute('aria-label')
                    if key == "phone":
                        match = re.search(r'Phone:\s*([\+\d\-\(\)\s]+)', aria_label)
                    else:  # Plus code
                        match = re.search(r'Plus code:\s*(.+)', aria_label)
                    if match:
                        business_details[key] = match.group(1).strip()
                    else:
                        inner_text = await element.inner_text()
                        business_details[key] = inner_text.strip()
                elif key == "rating":
                    aria_label = await element.get_attribute('aria-label')
                    match = re.search(r'([\d.]+)\s*stars?', aria_label)
                    business_details[key] = match.group(1) if match else "N/A"
                elif key == "reviews_count":
                    aria_label = await element.get_attribute('aria-label')
                    match = re.search(r'([\d,]+)\s*reviews?', aria_label)
                    business_details[key] = match.group(1).replace(',', '') if match else "N/A"
                else:
                    text = await element.inner_text()
                    business_details[key] = text.strip()
            else:
                business_details[key] = "N/A"
        except Exception as e:
            print(f"Error getting {key}: {e}")
            business_details[key] = "N/A"

    print(business_details)
    return business_details

async def interact_with_results(page):
    results_div_selector = "div[role='feed']"
    await page.wait_for_selector(results_div_selector, state="visible")
    result_item_selectors = "a[href^='https://www.google.com/maps']"
    result_items = await page.query_selector_all(result_item_selectors)

    businesses = []
    for item in result_items:
        try:
            await item.click()
            await asyncio.sleep(get_sleep_interval("short"))
            business_details = await get_business_details(page)
            businesses.append(business_details)
        except Exception as e:
            print(f"Error interacting with item: {e}")

    print(f"Interacted with {len(result_items)} items")
    return businesses
async def scroll_to_bottom(page):
    results_div_selector = "div[role='feed']"
    await page.wait_for_selector(results_div_selector, state="visible")

    while True:
        current_scroll_height = await page.evaluate(f"""
            (selector) => {{
                const element = document.querySelector(selector);
                return element.scrollHeight;
            }}
        """, results_div_selector)

        await page.evaluate(f"""
            (selector) => {{
                const element = document.querySelector(selector);
                element.scrollTo(0, element.scrollHeight);
            }}
        """, results_div_selector)

        await asyncio.sleep(get_sleep_interval("short"))

        new_scroll_height = await page.evaluate(f"""
            (selector) => {{
                const element = document.querySelector(selector);
                return element.scrollHeight;
            }}
        """, results_div_selector)

        if new_scroll_height == current_scroll_height:
            break
async def search_google_maps(page, lat, lng, niche):
    await page.goto(f"https://www.google.com/maps/search/{niche}/@{lat},{lng},12z")
    await asyncio.sleep(get_sleep_interval("medium"))
    await asyncio.sleep(get_sleep_interval("medium"))
    await scroll_to_bottom(page)
    await asyncio.sleep(get_sleep_interval("medium"))
    await interact_with_results(page)
    businesses = await interact_with_results(page)

    return businesses



def get_completed_steps(steps_folder, niche, state):
    print(steps_folder)
    completed_steps = set()
    if os.path.exists(steps_folder):
        for filename in os.listdir(steps_folder):
            if filename.startswith(f"{state}_{niche}_businesses_step_") and filename.endswith(".csv"):
                try:
                    step_number = int(filename.split("_")[-1].split(".")[0])
                    completed_steps.add(step_number)
                except ValueError:
                    print(f"Skipping invalid filename: {filename}")
    return completed_steps


async def maps_scrape(niche, x, y, state):
    top_left = {'lat': float(x.split(',')[0]), 'lng': float(x.split(',')[1])}
    bottom_right = {'lat': float(y.split(',')[0]), 'lng': float(y.split(',')[1])}
    step = calculate_steps(14)
    coordinates = generate_coordinates(top_left, bottom_right, step)

    data_folder = 'data'
    os.makedirs(data_folder, exist_ok=True)
    steps_folder = os.path.join(data_folder, 'steps')
    os.makedirs(steps_folder, exist_ok=True)
    completed_steps = get_completed_steps(steps_folder, niche, state)

    total_steps = len(coordinates)
    remaining_steps = [i for i in range(total_steps) if i + 1 not in completed_steps]
    num_remaining = len(remaining_steps)

    if num_remaining == 0:
        print("All steps have been completed. No further scraping needed.")
        verify_and_consolidate_data(niche, data_folder, state)
        return

    proxy_file_path = 'proxies/proxies.txt'
    all_proxies = parse_proxy_file(proxy_file_path)

    task_queue = asyncio.Queue()
    for step in remaining_steps:
        await task_queue.put(step)

    async def worker(state):
        while not task_queue.empty():
            try:
                step = await task_queue.get()
                lat, lng = coordinates[step]
                step_number = step + 1
                proxy = await get_verified_proxy(all_proxies)

                async with async_playwright() as p:
                    browser_args = {
                        'headless': True,
                        'proxy': {
                            "server": f"{proxy['ip']}:{proxy['port']}",
                            "username": proxy['username'],
                            "password": proxy['password']
                        }
                    }
                    browser = await p.chromium.launch(**browser_args)
                    context = await browser.new_context(viewport={"width": 1280, "height": 800})
                    page = await context.new_page()

                    print(f"Worker scanning location {step_number}/{len(coordinates)}: {lat}, {lng}")
                    businesses = await search_google_maps(page, lat, lng, niche)

                    step_filename = f"data/steps/{state}_{niche}_businesses_step_{step_number}.csv"
                    save_to_csv(businesses, filename=step_filename, mode='w',state=state)
                    print(f"Worker saved data for step {step_number} to {step_filename}")

                    save_to_csv(businesses, filename=f"data/all_{state}_{niche}_businesses.csv", mode='a',state=state)
                    print(f"Worker appended data from step {step_number} to all_{state}_{niche}_businesses.csv")

                    await asyncio.sleep(get_sleep_interval("long"))

                    await context.close()
                    await browser.close()

            except Exception as e:
                print(f"Error in worker for step {step_number}: {e}")

            finally:
                task_queue.task_done()
                await asyncio.sleep(get_sleep_interval("medium"))

    workers = [asyncio.create_task(worker(state)) for _ in range(MAX_WORKERS)]

    await task_queue.join()

    for w in workers:
        w.cancel()

    verify_and_consolidate_data(niche, data_folder, state)


def verify_and_consolidate_data(niche, data_folder, state):
    all_data = []
    steps_folder = os.path.join(data_folder, "steps")
    step_files = [f for f in os.listdir(steps_folder) if
                  f.startswith(f"{state}_{niche}_businesses_step_") and f.endswith(".csv")]

    for step_file in step_files:
        with open(os.path.join(steps_folder, step_file), 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            all_data.extend(list(reader))

    # Remove duplicates based on name and address
    unique_data = {(row['name'], row['address']): row for row in all_data}.values()

    # Save consolidated data
    consolidated_file = os.path.join(data_folder, f"consolidated_{state}_{niche}_businesses.csv")
    save_to_csv(list(unique_data), consolidated_file, 'w', state)
    print(f"Consolidated data saved to {consolidated_file}")

    # Print summary
    print(f"Total unique businesses found: {len(unique_data)}")