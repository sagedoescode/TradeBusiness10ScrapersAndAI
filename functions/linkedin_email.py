import asyncio
import csv
import json
import os
import re
from playwright.async_api import async_playwright, Playwright, expect
from functions.useful import get_sleep_interval
import random
import re

def clean_company_name(company_name):
    # Remove common suffixes
    suffixes = r'\s+(Inc\.?|LLC|Ltd\.?|Limited|Corp\.?|Corporation|Co\.?|Company|LLP|LP|P\.?C\.?|PLLC|PLC)$'
    cleaned_name = re.sub(suffixes, '', company_name, flags=re.IGNORECASE)

    # Remove any trailing commas and spaces
    cleaned_name = cleaned_name.rstrip(', ')

    return cleaned_name

async def human_typing(element, text, delay=30):
    for char in text:
        await element.type(char, delay=delay)
        await asyncio.sleep(get_sleep_interval("tiny"))

def load_profiles(PROFILES_JSON_PATH):
    if os.path.exists(PROFILES_JSON_PATH) and os.path.getsize(PROFILES_JSON_PATH) > 0:
        with open(PROFILES_JSON_PATH, 'r') as f:
            return json.load(f)
    return {}

async def search_and_get_email(page, target_name, target_company):
    search_input = page.get_by_placeholder("Search").filter(has_text="").nth(0)

    await search_input.click()


    await asyncio.sleep(get_sleep_interval("short"))
    await search_input.fill("")

    # Clean the company name
    cleaned_company = clean_company_name(target_company)
    print(target_company)
    # Construct the search query
    search_query = f"{target_name} {cleaned_company}"

    # Type the search query
    await human_typing(search_input, search_query)
    await search_input.press("Enter")

    await asyncio.sleep(get_sleep_interval("medium"))
    clicked = False

    # First layout: Hero card layout
    if not clicked:
        hero_card = await page.query_selector('.search-nec__hero-kcard-v2')
        if hero_card:
            profile_link = await hero_card.query_selector('a.app-aware-link')
            if profile_link:
                await profile_link.click()
                print('Clicked profile in hero card layout')
                clicked = True

    # Second layout: Entity result layout
    if not clicked:
        entity_result = await page.query_selector('.entity-result__item')
        if entity_result:
            profile_link = await entity_result.query_selector('a.app-aware-link')
            if profile_link:
                await profile_link.click()
                print('Clicked profile in entity result layout')
                clicked = True

    # Third layout: Reusable search layout
    if not clicked:
        reusable_search = await page.query_selector('.reusable-search__result-container')
        if reusable_search:
            profile_link = await reusable_search.query_selector('a.app-aware-link')
            if profile_link:
                await profile_link.click()
                print('Clicked profile in reusable search layout')
                clicked = True

    if not clicked:
        print('No matching layout found')
        return None, None

    await asyncio.sleep(get_sleep_interval("medium"))
    url = await page.evaluate("() => window.location.href")  #
    linkedin_url = url.replace("/overlay/contact-info/", "")
    await asyncio.sleep(get_sleep_interval("medium"))

    contact_info_link = page.get_by_role("link", name="Contact info")
    await contact_info_link.wait_for(state="visible", timeout=10000)
    await contact_info_link.click()

    await asyncio.sleep(get_sleep_interval("medium"))

    email_link = page.locator("section.pv-contact-info__contact-type a[href^='mailto:']")

    try:
        dismiss_button = page.locator('button[data-test-modal-close-btn]')
        if await dismiss_button.is_visible():
            await dismiss_button.click()
            print("Dismissed modal")
            await asyncio.sleep(get_sleep_interval("short"))
    except Exception as e:
        print(e)
    if await email_link.count() > 0:
        href = await email_link.get_attribute('href')
        email = href.replace('mailto:', '')
        print(f"Email found for {target_name} ({target_company}): {email}")
        return email, linkedin_url
    else:
        print(f"No email found for {target_name} ({target_company})")
        return None, linkedin_url

async def run_profile(playwright: Playwright, profile_name: str, targets: list,BASE_PROFILE_PATH, PROFILES_JSON_PATH) -> None:
    profiles = load_profiles(PROFILES_JSON_PATH)
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

    browser = await playwright.firefox.launch_persistent_context(user_data_dir=profile_path, headless=False, **browser_args)
    page = await browser.new_page()
    await page.goto("https://www.linkedin.com/")
    await asyncio.sleep(get_sleep_interval("medium"))

    # Check if already logged in
    if not await page.locator("[data-test-id=\"home-hero-sign-in-cta\"]").is_visible():
        print("Already logged in, skipping login process.")
    else:
        await page.locator("[data-test-id=\"home-hero-sign-in-cta\"]").click()
        await page.wait_for_load_state("networkidle")

        email_input = page.get_by_label("Email or phone")
        if await email_input.is_visible():
            await email_input.click()
            await human_typing(email_input, profile['email'])

            await asyncio.sleep(get_sleep_interval("short"))

            password_input = page.get_by_label("Password")
            await password_input.click()
            await human_typing(password_input, profile['password'])

            show_button = page.get_by_role("button", name="Show")
            if await show_button.is_visible():
                await show_button.click()

            await asyncio.sleep(get_sleep_interval("medium"))
            await page.get_by_role("button", name="Sign in").nth(0).click()
            await asyncio.sleep(get_sleep_interval("long"))
        else:
            print("Email input not found, assuming already logged in.")

    results = {}
    for target_name, target_company in targets:

        try:
            try:
                modal = page.locator('div[data-test-modal-container]')
                await modal.wait_for(state="visible", timeout=10000)
            except:
                pass
            # Try to click the "Dismiss" button
            try:
                dismiss_button = page.locator('button[aria-label="Dismiss"]')
                if await dismiss_button.is_visible():
                    await dismiss_button.click(timeout=5000)
                    print("Clicked Dismiss button")
            except:
                pass
            try:
                # If "Dismiss" button is not found, try "Maybe later" button
                maybe_later_button = page.locator('button:has-text("Maybe later")')
                if await maybe_later_button.is_visible():
                    await maybe_later_button.click(timeout=5000)
                    print("Clicked Maybe later button")
            except:
                pass

            # If neither button is found, try clicking outside the modal
            await page.mouse.click(0, 0)
            print("Clicked outside the modal")

        except Exception as e:
            print(f"An error occurred while trying to dismiss popup: {e}")
        try:
            email, linkedin_url = await search_and_get_email(page, target_name, target_company)
        except Exception as e:
            email = None
            linkedin_url =None
            print(f"Error {e}")
        if email is not None or linkedin_url is not None:
            results[(target_name, target_company)] = (email, linkedin_url)

    await browser.close()
    return results

async def get_emails(targets, BASE_PROFILE_PATH, PROFILES_JSON_PATH):
    profiles = load_profiles(PROFILES_JSON_PATH)
    if not profiles:
        print("No profiles found. Please create a profile first.")
        return

    print("Available profiles:")
    for profile_name in profiles:
        print(f"- {profile_name}")

    num_profiles = len(profiles)
    profile_names = list(profiles.keys())
    targets_per_profile = [[] for _ in range(num_profiles)]

    for i, target in enumerate(targets):
        profile_index = i % num_profiles
        targets_per_profile[profile_index].append(target)

    async with async_playwright() as playwright:
        tasks = [run_profile(playwright, profile_name, targets, BASE_PROFILE_PATH, PROFILES_JSON_PATH)
                 for profile_name, targets in zip(profile_names, targets_per_profile)]
        results = await asyncio.gather(*tasks)

    all_results = {}
    for result in results:
        if result:  # Add this check
            all_results.update(result)

    return all_results

def clean_company_name(company_name):
    pattern = r'\s+(Inc\.?|LLC|Ltd\.?|Limited|Corp\.?|Corporation|Co\.?|Company|LLP|LP|P\.?C\.?|PLLC|PLC)$'
    return re.sub(pattern, '', company_name, flags=re.IGNORECASE)


def process_csv(input_file):
    processed_data = []
    unique_owners = set()
    with open(input_file, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        fieldnames = reader.fieldnames + ['Personal Email'] if 'Personal Email' not in reader.fieldnames else reader.fieldnames
        if 'LinkedIn' not in fieldnames:
            fieldnames.append('LinkedIn')
        for row in reader:
            # Clean and process the data
            company_name = clean_company_name(row['Business Name'])
            owner_name = f"{row['Owner First Name']} {row['Owner Last Name']}".strip()

            processed_row = {
                'Category': row.get('Category', ''),
                'Language': row.get('Language', ''),
                'Business Name': company_name,
                'Phone #': row.get('Phone #', ''),
                'Phone # 2': row.get('Phone # 2', ''),
                'Website': row.get('Website', ''),
                'Site Rating': row.get('Site Rating', ''),
                'Reviews': row.get('Reviews', ''),
                'Rating': row.get('Rating', ''),
                'Owner First Name': row.get('Owner First Name', ''),
                'Owner Last Name': row.get('Owner Last Name', ''),
                'Owners Cel #': row.get('Owners Cel #', ''),
                'Owners Phone #': row.get('Owners Phone #', ''),
                'Personal Email': row.get('Personal Email', ''),
                'Business Email': row.get('Business Email', ''),
                'Owner Social Media': row.get('Owner Social Media', ''),
                'Owner Social Media 2': row.get('Owner Social Media 2', ''),
                'Instagram': row.get('Instagram', ''),
                'Facebook': row.get('Facebook', ''),
                'Linkedin': row.get('Linkedin', ''),
                'Business Address': row.get('Business Address', ''),
                'City': row.get('City', ''),
                'County': row.get('County', ''),
                'State': row.get('State', ''),
                'Google Link': row.get('Google Link', ''),
                'Plus Code': row.get('Plus Code', ''),
                'Source': row.get('Source', ''),
                'LinkedIn': row.get('LinkedIn', '')

            }

            processed_row['owner_name'] = owner_name  # Add this for internal use

            processed_data.append(processed_row)
            unique_owners.add(owner_name)

    return processed_data, fieldnames, list(unique_owners)

async def linkedin_email_get(niche, state):
    PROFILES_JSON_PATH = 'profiles_linkedin.json'
    BASE_PROFILE_PATH = r'C:\Users\Sage\PycharmProjects\MasterScraper\profiles'
    input_file = fr"C:\Users\Sage\PycharmProjects\MasterScraper\data\{state}_{niche}_central_stage_4.csv"
    output_file = fr"C:\Users\Sage\PycharmProjects\MasterScraper\data\updated_{state}_{niche}_businesses_with_owner_email.csv"
    progress_file = fr"C:\Users\Sage\PycharmProjects\MasterScraper\data\progress_{state}_{niche}_linkedin.csv"

    # Process the CSV file
    processed_data, fieldnames, unique_owners = process_csv(input_file)

    # Load progress if exists
    processed_owners = set()
    if os.path.exists(progress_file):
        with open(progress_file, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            processed_owners = set(row[0] for row in reader)

    # Create a dictionary to store owner names and their corresponding company names
    owner_company_dict = {}
    for row in processed_data:
        owner_name = row['owner_name']
        if owner_name not in owner_company_dict:
            owner_company_dict[owner_name] = row['Business Name']

    # Create targets from the unprocessed unique owners, including company names
    targets = [(owner_name, owner_company_dict[owner_name]) for owner_name in unique_owners if owner_name not in processed_owners]

    # Get emails for the targets
    results = await get_emails(targets, BASE_PROFILE_PATH, PROFILES_JSON_PATH)

    results = await get_emails(targets, BASE_PROFILE_PATH, PROFILES_JSON_PATH)

    if results:
        email_map = {owner_name: data[0] for (owner_name, _), data in results.items() if data[0]}
        linkedin_map = {owner_name: data[1] for (owner_name, _), data in results.items() if data[1]}
    else:
        print("No results returned from get_emails function.")
        email_map = {}
        linkedin_map = {}

    # Write the header to the output file if it doesn't exist
    if not os.path.exists(output_file):
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

    rows_with_email = 0
    total_rows = len(processed_data)

    # Process each row and update the file incrementally
    for row in processed_data:
        updated = False

        if row['owner_name'] in email_map:
            row['Personal Email'] = email_map[row['owner_name']]
            updated = True
            rows_with_email += 1

        if row['owner_name'] in linkedin_map:
            # Update LinkedIn only if it's empty or if we found a new URL
            if not row['LinkedIn'] or row['LinkedIn'] != linkedin_map[row['owner_name']]:
                row['LinkedIn'] = linkedin_map[row['owner_name']]
                updated = True

        # Remove 'owner_name' before writing to CSV
        row_to_write = {k: v for k, v in row.items() if k in fieldnames}

        # Append the row to the output file
        with open(output_file, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(row_to_write)

        if updated:
            print(
                f"Updated - Company: {row['Business Name']}, Owner: {row['owner_name']}, Email: {row['Personal Email']}")

        # Update progress file only for unique owners
        if row['owner_name'] not in processed_owners:
            with open(progress_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([row['owner_name']])
            processed_owners.add(row['owner_name'])


    # Print statistics
    print(f"\nTotal rows: {total_rows}")
    print(f"Unique owners: {len(unique_owners)}")
    print(f"Rows with email found: {rows_with_email}")
    print(f"Percentage of rows with email: {rows_with_email / total_rows * 100:.2f}%")
if __name__ == "__main__":
    asyncio.run(linkedin_email_get("Marble & Granite", "massachusetts"))