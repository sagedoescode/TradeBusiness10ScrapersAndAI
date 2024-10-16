import asyncio
import csv
import random
import re
import os
from playwright.async_api import async_playwright
from functions.useful import verify_proxy, get_random_proxy, parse_proxy_file


def decode_email(encoded_email, website):
    if website == 'facebook':
        email = encoded_email.replace('\\u0040', '@')
    else:
        email = encoded_email
    return email

async def get_verified_proxy(proxies):
    while True:
        proxy = get_random_proxy(proxies)
        if await verify_proxy(proxy):
            return proxy


async def dismiss_dialog(page):
    dismissal_methods = [
        # Method 1: Click dismiss button by aria-label
        lambda: page.click('button[aria-label="Dismiss"]'),

        # Method 2: Click close button by role
        lambda: page.click('div[role="dialog"] button[role="button"]'),

        # Method 3: Click outside the dialog
        lambda: page.mouse.click(0, 0),

        # Method 4: Use JavaScript to close the dialog
        lambda: page.evaluate('document.querySelector("div[role=\'dialog\']")?.remove()'),

        # Method 5: Press Escape key
        lambda: page.keyboard.press('Escape'),

        # Method 6: Click "Maybe later" button
        lambda: page.click('button:has-text("Maybe later")'),

        # Method 7: Use more generic selectors
        lambda: page.click('.x1n2onr6 button'),

        # Method 8: Click any button within the dialog
        lambda: page.evaluate(
            'Array.from(document.querySelectorAll("div[role=\'dialog\'] button")).find(b => b.offsetParent !== null)?.click()'),
    ]

    for method in dismissal_methods:
        try:
            await method()
            await asyncio.sleep(0.5)  # Wait a bit to see if the dialog disappears
            if not await page.is_visible('div[role="dialog"]'):
                print("Successfully dismissed dialog")
                return
        except Exception as e:
            pass  # Silently continue to the next method if this one fails

    print("Unable to dismiss dialog after trying all methods")


async def extract_emails(url, row, output_file, progress_set, website, PROXY_FILE_PATH, PROGRESS_FILE_PATH):
    if website == 'facebook':
        return False
    else:

        MAX_RETRIES = 2
    print(MAX_RETRIES)
    RETRY_DELAY = random.randint(1,2)
    all_proxies = parse_proxy_file(PROXY_FILE_PATH)

    for attempt in range(MAX_RETRIES):
        try:
            proxy = await get_verified_proxy(all_proxies)
            print(proxy)
            browser_args = {
                'headless': False,
                'proxy': {
                    "server": f"{proxy['ip']}:{proxy['port']}",
                    "username": proxy['username'],
                    "password": proxy['password']
                }
            }
            async with async_playwright() as p:
                browser = await p.chromium.launch(**browser_args)
                page = await browser.new_page()

                try:
                    if not url.startswith(('http://', 'https://')):
                        url = f'https://www.{url}'
                    elif url.startswith(('http://', 'https://')) and 'www.' not in url:
                        url = url.replace('://', '://www.')

                    print(f"Processing URL: {url} WEBSITE: {website} (Attempt {attempt + 1}/{MAX_RETRIES})")
                    # if website == 'facebook':
                    #     await page.goto("facebook.com", wait_until="domcontentloaded")
                    await page.goto(url, wait_until="domcontentloaded")
                    await dismiss_dialog(page)

                    content = await page.content()
                    email_pattern = r'\b[A-Za-z0-9._%+-]+\s*(?:@|\u0040|\\u0040)\s*[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'

                    encoded_emails = re.findall(email_pattern, content, re.IGNORECASE)

                    if encoded_emails:
                        decoded_email = decode_email(encoded_emails[0], website)
                        row['Business Email'] = decoded_email
                        print(f"Found email: {decoded_email}")
                        await update_output_file(row, output_file)
                        await update_progress(url, progress_set, PROGRESS_FILE_PATH)
                        return True
                    else:

                        print(f"No email addresses found on the page. (Attempt {attempt + 1}/{MAX_RETRIES})")
                        if attempt < MAX_RETRIES - 1:
                            print(f"Retrying in {RETRY_DELAY} seconds...")
                            await asyncio.sleep(RETRY_DELAY)
                        continue

                except Exception as e:

                    print(f"An error occurred: {str(e)} (Attempt {attempt + 1}/{MAX_RETRIES})")
                    if attempt < MAX_RETRIES - 1:
                        print(f"Retrying in {RETRY_DELAY} seconds...")
                        await asyncio.sleep(RETRY_DELAY)
                    continue

                finally:
                    await browser.close()

        except Exception as e:
            print(f"An error occurred while setting up the browser: {str(e)} (Attempt {attempt + 1}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES - 1:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                await asyncio.sleep(RETRY_DELAY)
            continue

    print(f"Max retries reached for URL: {url}. No email found.")
    return False

async def update_output_file(row, output_file):
    await asyncio.to_thread(write_csv_row, output_file, row)

def write_csv_row(file, row):
    writer = csv.DictWriter(file, fieldnames=row.keys())
    writer.writerow(row)
    file.flush()
    os.fsync(file.fileno())

async def update_progress(url, progress_set, PROGRESS_FILE_PATH):
    progress_set.add(url)
    async with asyncio.Lock():
        await asyncio.to_thread(append_to_file, PROGRESS_FILE_PATH, f"{url}\n")

def append_to_file(file_path, content):
    with open(file_path, 'a') as file:
        file.write(content)
        file.flush()
        os.fsync(file.fileno())


async def process_row(row, output_file, progress_set, PROGRESS_FILE_PATH, PROXY_FILE_PATH):
    email_found = False

    if row['Facebook'] and row['Facebook'] not in progress_set:
        try:
            email_found = await extract_emails(row['Facebook'], row, output_file, progress_set, 'facebook', PROXY_FILE_PATH,PROGRESS_FILE_PATH)
        except Exception as e:
            print(f"Error processing Facebook URL: {str(e)}")

    if not email_found and row['Website'] and row['Website'] not in progress_set:
        try:
            email_found = await extract_emails(row['Website'], row, output_file, progress_set, 'Website', PROXY_FILE_PATH,PROGRESS_FILE_PATH)
        except Exception as e:
            print(f"Error processing Website URL: {str(e)}")

    if not email_found:
        print(f"No email found for row: {row}")
        row['Business Email'] = ''
        await update_output_file(row, output_file)


async def scrape_websites_general_facebook_email(state, niche):
    print("Starting the main function")  # Debug print

    PROXY_FILE_PATH = r'C:\Users\Sage\PycharmProjects\MasterScraper/proxies/proxies.txt'
    INPUT_CSV_PATH = fr'C:\Users\Sage\PycharmProjects\MasterScraper\data\{state}_{niche}_central_stage_4.csv'
    OUTPUT_CSV_PATH = fr'C:\Users\Sage\PycharmProjects\MasterScraper/data/{state}_{niche}_scraped_emails.csv'
    PROGRESS_FILE_PATH = fr'C:\Users\Sage\PycharmProjects\MasterScraper\data\{state}_{niche}_scraping_emails_progress.txt'
    MAX_WORKERS = 20
    total_links = 0
    processed_links = 0

    # Create progress file if it doesn't exist
    if not os.path.exists(PROGRESS_FILE_PATH):
        open(PROGRESS_FILE_PATH, 'w').close()
    print(f"Progress file checked/created at {PROGRESS_FILE_PATH}")

    with open(PROGRESS_FILE_PATH, 'r') as progress_file:
        progress_set = set(line.strip() for line in progress_file)
    print(f"Loaded {len(progress_set)} items from progress file")

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(OUTPUT_CSV_PATH), exist_ok=True)
    print(f"Output directory ensured at {os.path.dirname(OUTPUT_CSV_PATH)}")

    # Check if output file exists and read existing data
    existing_data = {}
    if os.path.exists(OUTPUT_CSV_PATH):
        with open(OUTPUT_CSV_PATH, 'r', newline='') as existing_file:
            reader = csv.DictReader(existing_file)
            for row in reader:
                existing_data[row.get('Facebook', '') or row.get('Website', '')] = row
    print(f"Loaded {len(existing_data)} existing entries from output file")

    # Open input file to get fieldnames and count total links
    with open(INPUT_CSV_PATH, 'r', newline='') as input_file:
        reader = csv.DictReader(input_file)
        fieldnames = reader.fieldnames + ['Email']
        for row in reader:
            if (row.get('Facebook') and row.get('Facebook') not in progress_set) or (
                    row.get('Website') and row.get('Website') not in progress_set):
                total_links += 1
    print(f"Loaded fieldnames from input file: {fieldnames}")
    print(f"Total links to process: {total_links}")

    # Open output file in append mode
    with open(OUTPUT_CSV_PATH, 'a', newline='') as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)

        # Write header only if the file is empty
        if output_file.tell() == 0:
            writer.writeheader()
            print("Wrote header to output file")

        tasks = set()
        with open(INPUT_CSV_PATH, 'r', newline='') as input_file:
            reader = csv.DictReader(input_file)
            for row in reader:
                if row.get('Facebook') in existing_data or row.get('Website') in existing_data:
                    continue
                if (row.get('Facebook') and row.get('Facebook') not in progress_set) or (
                        row.get('Website') and row.get('Website') not in progress_set):
                    if len(tasks) >= MAX_WORKERS:
                        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                        tasks = pending
                        processed_links += len(done)
                        print(f"Progress: {processed_links}/{total_links} links processed")
                    task = asyncio.create_task(
                        process_row(row, output_file, progress_set, PROGRESS_FILE_PATH, PROXY_FILE_PATH))
                    tasks.add(task)

        if tasks:
            print(f"Waiting for {len(tasks)} tasks to complete")
            done, _ = await asyncio.wait(tasks)
            processed_links += len(done)
            print(f"Final Progress: {processed_links}/{total_links} links processed")
        else:
            print("No tasks were created. Check if there are any rows to process.")

    print("Main function completed")


# asyncio.run(scrape_websites_general_facebook_email(state="Massachusetts", niche="Roofing"))