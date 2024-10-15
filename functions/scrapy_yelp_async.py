import pandas as pd
import aiohttp
import asyncio
from time import sleep
import random
import json
import os
from functions.useful import get_state_abbreviations

async def scrape_yellp(state, term):
    # Load the CSV file containing the cities
    state_abbreviations = get_state_abbreviations()
    state_abbr = state_abbreviations.get(state.lower())
    url = 'https://raw.githubusercontent.com/grammakov/USA-cities-and-states/master/us_cities_states_counties.csv'
    df = pd.read_csv(url, sep='|', engine='pyarrow')
    print(df.head())
    # Filter the cities of the given state
    state_cities = df[df['State short'] == state_abbr]
    print(f"4. Filtered cities for {state}")

    # Listing the cities
    cities = list(set([city for city in state_cities['City']]))
    print(f"5. Number of cities: {len(cities)}")

    # Create a progress file name
    progress_file = f"progress_{state}_{term}_yelp.json"

    # Load progress if it exists
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            progress = json.load(f)
    else:
        progress = {}

    tokens = [
        "UxXLbAXPGNzD-_d4e5MSLiMkkCPNyaZAdZEpwGnfX1Ycm7XuXjQPfiB9Ct63_TnsuSls-TgE9jqbtYi36REmiLD5qpZ8Wz8BsuetWPkdlkja2nSsKlcd249Yk98CZ3Yx",
        # ... (other tokens)
    ]

    url = "https://api.yelp.com/v3/businesses/search"

    limit = 20  # Maximum allowed value

    list_remove_keys = ['business_hours', 'coordinates', 'transactions']
    list_items = []

    async def fetch(session, location, token):
        if location in progress and progress[location]:
            print(f"Skipping {location} - already scraped")
            return

        offset = 0  # initial value
        length_location = 0  # length data
        count = 0  # count try tokens
        print(f"Fetching data for {location}")
        while True:
            params = {
                'limit': limit,
                'offset': offset,
                'location': location,
                'term': term,
                'sort_by': 'best_match'
            }

            headers = {"accept": "application/json", "Authorization": "Bearer " + token}
            async with session.get(url, headers=headers, params=params) as response:
                print(f"Request for {location}: status {response.status}")
                if response.status not in [200, 429]:
                    print(f"Error: {location} - {response.status}, {await response.text()} ")
                    break

                elif response.status == 429:
                    if count == 10: break
                    print("Change Token request")
                    count += 1
                    continue

                data = await response.json()

                # Extract data and add to the list
                for item in data['businesses']:
                    temp_dict = item.copy()
                    temp_dict['categories'] = ", ".join([i['title'] for i in item['categories']])

                    if isinstance(item['location'], list):
                        temp_dict['location_address1'] = ", ".join([i['address1'] for i in item['location']])
                        temp_dict['location_city'] = ", ".join([i['city'] for i in item['location']])
                        temp_dict['location_zip_code'] = ", ".join([i['zip_code'] for i in item['location']])
                    else:
                        temp_dict['location_address1'] = item['location']['address1']
                        temp_dict['location_city'] = item['location']['city']
                        temp_dict['location_zip_code'] = item['location']['zip_code']
                    del temp_dict['location']

                    if isinstance(item['attributes'], list):
                        temp_dict['business_url'] = ", ".join([i.get('business_url') for i in item['attributes']])
                    else:
                        temp_dict['business_url'] = item['attributes'].get('business_url')
                    del temp_dict['attributes']

                    for k, v in item.items():
                        if k in list_remove_keys:
                            del temp_dict[k]
                    list_items.append(temp_dict)

                # Update length
                length_location += len(data['businesses'])

                # If the number of results returned is less than the limit, it means all data has been fetched
                if len(data['businesses']) < limit:
                    break

                # Increment the offset to fetch the next results
                offset += limit

                # The API may have rate limits, so include a delay to avoid being blocked
                await asyncio.sleep(1)  # Pause for 1 second between requests

        print(f"Total results found for city {location}: {length_location}")

        # Mark this location as scraped in the progress file
        progress[location] = True
        with open(progress_file, 'w') as f:
            json.dump(progress, f)

    async def main():
        print("6. Starting main function")
        async with aiohttp.ClientSession() as session:
            tasks = []
            for location in cities:
                token = random.choice(tokens)
                tasks.append(fetch(session, location, token))
            print(f"7. Created {len(tasks)} tasks")
            try:
                results = await asyncio.gather(*tasks)
                print("8. All tasks completed")
            except Exception as e:
                print(f"Error in main: {e}")

            print("9. Finished main function")

    # Check if there's already a running event loop
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        print('10. Async event loop already running. Adding coroutine to the event loop.')
        tsk = loop.create_task(main())
        await tsk
    else:
        print('10. Starting new event loop')
        await main()

    print("11. Creating DataFrame")
    df_search = pd.DataFrame(list_items)
    df_search = df_search.drop_duplicates()
    df_search.to_csv(fr"C:\Users\Sage\PycharmProjects\MasterScraper\data/yelp_{state}_{term}.csv", index=False)
    print(f"12. CSV file saved {fr"C:\Users\Sage\PycharmProjects\MasterScraper\data/yelp_{state}_{term}.csv"}")

    return df_search

async def scrape_yelp(state, term):
    print(f"Starting scrape for {state} {term}")
    try:
        result = await scrape_yellp(state, term)
        print(f"Scraped data for {state} {term}:")
        print(result.head())
    except Exception as e:
        print(f"Error in scrape_yelp: {e}")
    print(f"Finished scrape for {state} {term}")

# Run the script
if __name__ == "__main__":
    state = "Massachusetts"
    term = "Marble & Granite"
    asyncio.run(scrape_yelp(state, term))