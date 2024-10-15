import os
import csv
import shutil
import time
import asyncio
from datetime import datetime, timedelta

import google
from dotenv import load_dotenv
import google.generativeai as genai
from google.api_core.exceptions import ResourceExhausted

class AsyncRateLimiter:
    def __init__(self, rpm_limit, rpd_limit):
        self.rpm_limit = rpm_limit
        self.rpd_limit = rpd_limit
        self.requests = []
        self.daily_requests = 0
        self.last_reset = datetime.now()
        self.lock = asyncio.Lock()

    async def wait_if_needed(self):
        async with self.lock:
            now = datetime.now()

            if now.date() > self.last_reset.date():
                self.daily_requests = 0
                self.last_reset = now

            if self.daily_requests >= self.rpd_limit:
                print("Daily limit reached. Waiting until midnight...")
                await asyncio.sleep(
                    (now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1) - now).total_seconds())
                self.daily_requests = 0
                self.last_reset = datetime.now()

            self.requests = [req for req in self.requests if (now - req).total_seconds() < 60]

            if len(self.requests) >= self.rpm_limit:
                sleep_time = 60 - (now - self.requests[0]).total_seconds()
                if sleep_time > 0:
                    print(f"Rate limit reached. Waiting for {sleep_time:.2f} seconds...")
                    await asyncio.sleep(sleep_time)

            self.requests.append(now)
            self.daily_requests += 1

async def get_nationality(full_name, rate_limiter):
    load_dotenv()
    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
    model = genai.GenerativeModel("gemini-1.5-flash-latest")

    await rate_limiter.wait_if_needed()
    print("calling gemini")
    response = await asyncio.to_thread(
        model.generate_content,
        f"Here's a name of an owner of business in USA make assumption on what language he speaks like a human would guess. The name is {full_name}. Return single word his possible heritage"
    )
    print(f"response{response.text}")
    return response.text.strip()

async def process_row(row, writer, rate_limiter):
    if not isinstance(row, dict):
        print(f"Unexpected row type: {type(row)}. Row content: {row}")
        return

    owner_first_name = row.get('Owner First Name', '').strip() if row.get('Owner First Name') else ''
    owner_last_name = row.get('Owner Last Name', '').strip() if row.get('Owner Last Name') else ''
    full_name = f"{owner_first_name} {owner_last_name}".strip()

    assumed_heritage = row.get("Assumed Heritage", "").strip() if row.get("Assumed Heritage") else ""

    if full_name and assumed_heritage == "":
        print(f"Processing: {full_name}")
        try:
            heritage = await get_nationality(full_name, rate_limiter)
            row["Assumed Heritage"] = heritage
            print(f"Name: {full_name}, Heritage: {heritage}")
        except google.api_core.exceptions.ResourceExhausted as e:
            print(f"ResourceExhausted error occurred: {str(e)}")
            print("Details:", e.details())
            print("Retrying in 120 seconds...")
            await asyncio.sleep(120)
            try:
                heritage = await get_nationality(full_name, rate_limiter)
                row["Assumed Heritage"] = heritage
                print(f"Retry successful. Name: {full_name}, Heritage: {heritage}")
            except google.api_core.exceptions.ResourceExhausted as e:
                print(f"ResourceExhausted error occurred again: {str(e)}")
                print("Details:", e.details())
                print("Appending empty string for heritage and continuing with remaining rows.")
                row["Assumed Heritage"] = ""
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")
            row["Assumed Heritage"] = ""
    else:
        row["Assumed Heritage"] = assumed_heritage

    writer.writerow(row)

async def process_csv_heritage(state, niche):
    input_file = fr"C:\Users\Sage\PycharmProjects\MasterScraper\data\updated_{state}_{niche}_businesses_with_socials.csv"
    temp_file = fr"C:\Users\Sage\PycharmProjects\MasterScraper\data\temp_{state}_{niche}_businesses_with_socials_with_heritage.csv"
    backup_file = fr"C:\Users\Sage\PycharmProjects\MasterScraper\data\backup_{state}_{niche}_businesses_with_socials.csv"

    shutil.copy2(input_file, backup_file)
    rate_limiter = AsyncRateLimiter(rpm_limit=15, rpd_limit=1500)

    try:
        with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
                open(temp_file, 'w', newline='', encoding='utf-8') as outfile:

            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames
            print("Fieldnames:", fieldnames)
            if "Assumed Heritage" not in fieldnames:
                fieldnames.append("Assumed Heritage")

            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            tasks = []
            for row in reader:
                task = asyncio.create_task(process_row(row, writer, rate_limiter))
                tasks.append(task)

            await asyncio.gather(*tasks)

        os.replace(temp_file, input_file)
        print("Processing completed successfully. Check the output file for results.")
    except Exception as e:
        print(f"An error occurred during processing: {str(e)}")
        print("Restoring the original file from backup...")
        os.replace(backup_file, input_file)
        print("Original file restored. No changes were made.")
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)
        if os.path.exists(backup_file):
            os.remove(backup_file)

    print("Processing completed. Check the output file for results.")

async def get_heritage(state, niche):
    await process_csv_heritage(state=state, niche=niche)

asyncio.run(get_heritage(state="massachusetts", niche="Counter Top"))