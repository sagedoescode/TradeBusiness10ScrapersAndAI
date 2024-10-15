import asyncio
import json
import os
import csv
import re

from playwright.async_api import async_playwright, Playwright
from functions.useful import get_sleep_interval


def create_output_csv(input_file):
    base_name = os.path.basename(input_file)
    name_without_ext = os.path.splitext(base_name)[0]
    output_file = os.path.join(os.path.dirname(input_file), f"{name_without_ext}_with_owners.csv")
    return output_file

def load_and_update_csv(input_file, output_file, company, unique_names):
    if not os.path.exists(output_file):
        # If output file doesn't exist, create it with headers from input file
        with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
             open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

    with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
         open(output_file, 'a', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        print(reader)
        print(fieldnames)
        print(company)
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)

        company_name_key = 'Business Name'  # Assume 'name' is the key for company name
        if 'Business Name' in fieldnames:
            company_name_key = 'Business Name'
        elif 'Business Name' not in fieldnames:
            print(f"Warning: Could not find a suitable company name column. Available columns: {fieldnames}")
            return

        for row in reader:
            if row[company_name_key] == company:
                for name in unique_names:
                    print(name)
                    new_row = row.copy()
                    name_parts = name.split(maxsplit=1)
                    new_row['Owner First Name'] = name_parts[0]
                    new_row['Owner Last Name'] = name_parts[1] if len(name_parts) > 1 else ''
                    writer.writerow(new_row)

def load_profiles(PROFILES_JSON_PATH):
    if os.path.exists(PROFILES_JSON_PATH) and os.path.getsize(PROFILES_JSON_PATH) > 0:
        with open(PROFILES_JSON_PATH, 'r') as f:
            return json.load(f)
    return {}

def load_progress(PROGRESS_FILE):
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_progress(progress, PROGRESS_FILE):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)




async def extract_unique_names(page):
    officer_elements = await page.locator("dd.officers ul.attribute_list li.attribute_item").all()
    unique_names = set()
    for element in officer_elements:
        name_and_role = await element.inner_text()
        match = re.match(r"(.*?),\s*(.*)", name_and_role)
        if match:
            name = match.group(1).strip()
            unique_names.add(name)

    return list(unique_names)[:3]
async def human_typing(element, text, delay=30):
    for char in text:
        await element.type(char, delay=delay)
        await asyncio.sleep(get_sleep_interval("tiny"))





async def login(page, profile):
    print("Checking if logged in")
    my_account_link = page.get_by_role("link", name="My Account")
    if await my_account_link.is_visible():
        print("Already logged in.")
        return

    # If not logged in, proceed with login
    await page.get_by_role("link", name="Log in/Sign up").click()
    await asyncio.sleep(get_sleep_interval("medium"))
    await asyncio.sleep(get_sleep_interval("medium"))
    print("Checking if logged in")
    if await my_account_link.is_visible():
        print("Already logged in.")
        return
    await page.get_by_role("link", name=" Linkedin").click()
    print("Checking if logged in")
    if await my_account_link.is_visible():
        print("Already logged in.")
        return
    await asyncio.sleep(get_sleep_interval("medium"))
    await asyncio.sleep(get_sleep_interval("medium"))
    await asyncio.sleep(get_sleep_interval("medium"))
    await asyncio.sleep(get_sleep_interval("medium"))
    try:
        email_input = page.get_by_label("Email or Phone")
        await email_input.click()
        await email_input.fill("")
        await human_typing(email_input, profile['email'])
    except:
        print("Error while logging in or already logged in")
        return

    password_input = page.get_by_label("Password")
    await password_input.click()
    await human_typing(password_input, profile['password'])

    await page.get_by_label("Sign in").click()
    await page.wait_for_load_state("networkidle")



async def run_profile(playwright: Playwright, profile_name: str, companies: list, input_file: str, output_file: str, BASE_PROFILE_PATH, PROGRESS_FILE, PROFILES_JSON_PATH) -> list:
    results = []
    profiles = load_profiles(PROFILES_JSON_PATH)
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

    browser = await playwright.firefox.launch_persistent_context(user_data_dir=profile_path, headless=False,
                                                                 **browser_args)

    page = await browser.new_page()
    await page.goto("https://opencorporates.com/companies")
    await asyncio.sleep(get_sleep_interval("medium"))
    await login(page, profile)

    for company in companies:
        try:
            result = await scrape_company(page, company, input_file, output_file)
            if result:
                results.append(result)
                print(f"Successfully scraped data for {company}")
                # Save progress after each successful scrape
                progress = load_progress(PROGRESS_FILE)
                progress[company] = result
                save_progress(progress,PROGRESS_FILE)
                print(f"Saved progress for {company}")
            else:
                print(f"No data found for {company}")
        except Exception as e:
            print(f'Error processing company {company}')
            continue


    await browser.close()
    return results

def normalize_company_name(name):
    # Convert to lowercase
    name = name.lower()
    # Replace '&' or 'and' with a space
    name = re.sub(r'&|and', ' ', name)
    # Replace common suffixes
    name = re.sub(r'\b(inc|llc|ltd|co)\b', '', name)
    # Remove all non-alphanumeric characters
    name = re.sub(r'[^a-z0-9]', '', name)
    return name.strip()
async def scrape_company(page, target_company, input_file, output_file):
    await asyncio.sleep(get_sleep_interval("medium"))
    try:
        search_input = page.get_by_placeholder("Company name or number")
        await search_input.click()
    except:
        await page.reload()
        await asyncio.sleep(get_sleep_interval("medium"))
        await asyncio.sleep(get_sleep_interval("medium"))
        await asyncio.sleep(get_sleep_interval("medium"))
        await asyncio.sleep(get_sleep_interval("medium"))
        await asyncio.sleep(get_sleep_interval("medium"))
        await asyncio.sleep(get_sleep_interval("medium"))
        await asyncio.sleep(get_sleep_interval("medium"))
        await asyncio.sleep(get_sleep_interval("medium"))
        await asyncio.sleep(get_sleep_interval("medium"))
        search_input = page.get_by_placeholder("Company name or number")
        await search_input.click()
    await asyncio.sleep(get_sleep_interval("short"))
    await human_typing(search_input, target_company)
    await search_input.press("Enter")

    await asyncio.sleep(get_sleep_interval("medium"))
    await asyncio.sleep(get_sleep_interval("medium"))
    await asyncio.sleep(get_sleep_interval("medium"))
    await asyncio.sleep(get_sleep_interval("medium"))
    results_div = page.locator("div#results")
    if await results_div.is_visible():
        companies_list = results_div.locator("ul#companies")
        search_results = companies_list.locator("li.search-result")

        normalized_target = normalize_company_name(target_company)
        best_match = None
        best_match_score = 0

        active_results = []
        inactive_results = []

        for result in await search_results.all():
            print(result)
            company_link = result.locator("a.company_search_result")
            company_name = await company_link.inner_text()
            normalized_name = normalize_company_name(company_name)

            # Calculate similarity score
            score = len(set(normalize_company_name(target_company)) & set(normalized_name)) / max(
                len(normalize_company_name(target_company)), len(normalized_name))
            try:
                is_inactive = await result.locator("span.status.label").inner_text() == "inactive"
                print("Inactive")
            except:
                is_inactive = False
                print("Active")
            if score > 0.4:
                if is_inactive:
                    inactive_results.append((result, score))
                else:
                    active_results.append((result, score))

        # Sort results by score in descending order
        active_results.sort(key=lambda x: x[1], reverse=True)
        inactive_results.sort(key=lambda x: x[1], reverse=True)

        result_to_click = None

        if active_results:
            result_to_click = active_results[0][0]
        elif inactive_results:
            print(f"Warning: Only inactive results found for {target_company}")
            return None

        if result_to_click:
            await result_to_click.locator("a.company_search_result").click()

            current_url = page.url
            if "google.com" in current_url:
                print(f"Clicked on a Google link for {target_company}. Skipping.")
                await page.goto("https://opencorporates.com/companies")  # Go back to search page
                return None

            print(f"Clicked on search result for {target_company}")

        else:
            print(f"No suitable match found for {target_company}")
            return None

    await asyncio.sleep(get_sleep_interval("medium"))

    unique_names = await extract_unique_names(page)
    print(f"\nFirst 3 Unique Officers for {target_company}:")
    for name in unique_names:
        print(f"- {name}")
    save_company_data(input_file, output_file, target_company, unique_names)
    #load_and_update_csv(input_file, output_file, target_company, unique_names)
    result = {
        "company": target_company,
        "officers": unique_names
    }

    # Navigate back to the search page for the next company
    await page.goto("https://opencorporates.com/companies")
    await asyncio.sleep(get_sleep_interval("medium"))

    return result
def save_company_data(input_file, output_file, company, unique_names):
    if not os.path.exists(output_file):
        # If output file doesn't exist, create it with headers from input file
        with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
                open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

    with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
            open(output_file, 'a', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        print(reader)
        print(fieldnames)
        print(company)
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)

        company_name_key = 'Business Name'  # Change this to match your actual CSV header
        if company_name_key not in fieldnames:
            print(f"Warning: Could not find '{company_name_key}' column. Available columns: {fieldnames}")
            return

        for row in reader:
            if row[company_name_key] == company:
                for name in unique_names:
                    new_row = row.copy()
                    name_parts = name.split(maxsplit=1)
                    new_row['Owner First Name'] = name_parts[0]
                    new_row['Owner Last Name'] = name_parts[1] if len(name_parts) > 1 else ''
                    writer.writerow(new_row)

    print(f"Data saved for {company}")
async def get_company_info(niche, state):
    PROFILES_JSON_PATH = 'profiles_linkedin.json'
    BASE_PROFILE_PATH = r'C:\Users\Sage\PycharmProjects\MasterScraper\profiles'
    PROGRESS_FILE = f'progress_opencorps_{state}_{niche}.json'
    OUTPUT = fr"C:\Users\Sage\PycharmProjects\MasterScraper\data\updated_{state}_{niche}_businesses_with_socials_OPENCORPS.csv"
    input_file = fr"C:\Users\Sage\PycharmProjects\MasterScraper\data\{state}_{niche}_central_stage_1.csv"
    output_file = create_output_csv(OUTPUT)
    profiles = load_profiles(PROFILES_JSON_PATH)
    if not profiles:
        print("No profiles found. Please create a profile first.")
        return

    # Load companies that are already in the output file
    existing_companies = set()
    if os.path.exists(output_file):
        with open(output_file, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            existing_companies = set(row['Business Name'] for row in reader)

    companies = []
    with open(input_file, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header
        companies = [row[2] for row in reader if row[2] not in existing_companies]  # Assuming company name is in the first column

    progress = load_progress(PROGRESS_FILE)
    companies = [company for company in companies if company not in progress]

    print(f"Total companies to scrape: {len(companies)}")

    num_profiles = len(profiles)
    companies_per_profile = len(companies) // num_profiles + (1 if len(companies) % num_profiles else 0)

    profile_tasks = []

    async with async_playwright() as playwright:
        for i, profile_name in enumerate(profiles.keys()):
            start = i * companies_per_profile
            end = min((i + 1) * companies_per_profile, len(companies))
            profile_companies = companies[start:end]

            if profile_companies:
                task = asyncio.create_task(
                    run_profile(playwright, profile_name, profile_companies, input_file, output_file, BASE_PROFILE_PATH, PROGRESS_FILE, PROFILES_JSON_PATH))
                profile_tasks.append(task)

        results = await asyncio.gather(*profile_tasks)

    for profile_results in results:
        for result in profile_results:
            progress[result['company']] = result

    save_progress(progress, PROGRESS_FILE)
    print(f"Scraped and saved data for {len(progress)} companies.")


if __name__ == "__main__":
    asyncio.run(get_company_info("Roofing", "massachusetts"))