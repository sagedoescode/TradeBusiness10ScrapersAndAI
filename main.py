import asyncio
import time
import logging
from datetime import datetime

import google
from google.api_core.exceptions import ResourceExhausted
from functions.export_maps_gs import export
from functions.get_sheet_data import get_states, get_niches
from functions.google_search import social_google_search
from functions.ma import ma_sec
from functions.merge_csv_files import merge_csv_files, merge_complementary_rows
from functions.parse_local import get_heritage_local
from functions.thumbtack_script2 import thumbtack_scraper
from functions.scrapy_yelp_async import scrape_yelp
from functions.anywho import main_scraper_anywho
from functions.open_corps import get_company_info
from functions.better_business_bureau import run_bbb_scraper
from maps import maps_scrape
from functions.facebook import scrape_websites_general_facebook_email
from functions.linkedin_email import linkedin_email_get

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='script_execution.log',
    filemode='a'
)

def log_execution(func):
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            logging.info(f"{func.__name__} executed successfully. Execution time: {execution_time:.2f} seconds.")
            return result
        except Exception as e:
            logging.error(f"Error in {func.__name__}: {str(e)}")
            raise

    return wrapper

@log_execution
async def wrapped_maps_scrape(*args, **kwargs):
    return await maps_scrape(*args, **kwargs)

@log_execution
async def wrapped_ma_sec(*args, **kwargs):
    return await ma_sec(*args, **kwargs)

@log_execution
async def wrapped_social_google_search(*args, **kwargs):
    return await social_google_search(*args, **kwargs)

@log_execution
async def wrapped_get_heritage(*args, **kwargs):
    return await get_heritage_local(*args, **kwargs)

@log_execution
async def wrapped_export(*args, **kwargs):
    return export(*args, **kwargs)

@log_execution
async def wrapped_thumbtack_scraper(*args, **kwargs):
    return await thumbtack_scraper(*args, **kwargs)

@log_execution
async def wrapped_scrape_yelp(*args, **kwargs):
    return await scrape_yelp(*args, **kwargs)

@log_execution
async def wrapped_main_scraper_anywho(*args, **kwargs):
    return await main_scraper_anywho(*args, **kwargs)

@log_execution
async def wrapped_get_company_info(*args, **kwargs):
    return await get_company_info(*args, **kwargs)

@log_execution
async def wrapped_run_bbb_scraper(*args, **kwargs):
    return await run_bbb_scraper(*args, **kwargs)

@log_execution
async def wrapped_scrape_facebook(*args, **kwargs):
    return await scrape_websites_general_facebook_email(*args, **kwargs)

@log_execution
async def wrapped_get_linkedin_emails(*args, **kwargs):
    return await linkedin_email_get(*args, **kwargs)

async def run_with_retries(func, *args, **kwargs):
    max_retries = 3
    retry_count = 0
    while retry_count < max_retries:
        try:
            return await func(*args, **kwargs)
        except google.api_core.exceptions.ResourceExhausted as e:
            retry_count += 1
            logging.error(f"ResourceExhausted error in {func.__name__} (attempt {retry_count}): {str(e)}")
            if isinstance(e.details, list):
                logging.error(f"Error details: {', '.join(e.details)}")
            else:
                logging.error(f"Error details: {str(e.details)}")
            if retry_count < max_retries:
                wait_time = 240 * retry_count  # Increase wait time with each retry
                logging.info(f"Retrying {func.__name__} in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            else:
                logging.error(f"Max retries reached for {func.__name__}. Moving to next function.")
        except Exception as e:
            retry_count += 1
            logging.error(f"Error in {func.__name__} (attempt {retry_count}): {str(e)}")
            if retry_count < max_retries:
                wait_time = 240 * retry_count  # Increase wait time with each retry
                logging.info(f"Retrying {func.__name__} in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            else:
                logging.error(f"Max retries reached for {func.__name__}. Moving to next function.")

async def main():
    logging.info("Script started.")
    while True:
        try:
            states = get_states()
            niches = get_niches()
            logging.info(f"Successfully retrieved {len(states)} states and {len(niches)} niches.")
            break
        except Exception as e:
            logging.error(f"Error retrieving states and niches: {str(e)}")
            await asyncio.sleep(2)

    for state, x, y in states:
        logging.info(f"Processing state: {state}, X: {x}, Y: {y}")
        for niche in niches:
            logging.info(f"Processing niche: {niche}")


            group1 = [
                run_with_retries(wrapped_thumbtack_scraper, state, niche),
                run_with_retries(wrapped_scrape_yelp, state, niche),
                run_with_retries(wrapped_maps_scrape, niche, x, y, state),
                run_with_retries(wrapped_run_bbb_scraper, niche, state)
            ]
            await asyncio.gather(*group1)
            # Merge after Group 1
            merge_csv_files(state, niche, 1)

            # Group 2
            if state.lower() == "massachusetts":
                await run_with_retries(wrapped_ma_sec, niche, state)
            else:
                await run_with_retries(wrapped_get_company_info, niche, state)

            # Merge after Group 2
            merge_csv_files(state, niche, 2)

            # Group 3
            await run_with_retries(wrapped_social_google_search, state, niche, max_workers=10)

            # Merge after Group 3
            merge_csv_files(state, niche, 3)

            # Group 4
            await run_with_retries(wrapped_get_heritage, state, niche)

            # Merge after Group 4
            merge_csv_files(state, niche, 4)

            # Group 5
            group5 = [
                run_with_retries(wrapped_main_scraper_anywho, state, niche),

                run_with_retries(wrapped_get_linkedin_emails, niche, state)
            ]
            await asyncio.gather(*group5)

            # Merge after Group 5
            merge_csv_files(state, niche, 5)
            merge_complementary_rows(state, niche)
            await run_with_retries(wrapped_export, niche, state)

    logging.info("Script completed.")

if __name__ == "__main__":
    asyncio.run(main())