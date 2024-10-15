import asyncio
import csv
import os
import re

import math
import random

import aiohttp


def clean_text(text):
    # Regular expression to match emojis and other special characters
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U0001F1F2-\U0001F1F4"  # Macau flag
                               u"\U0001F1E6-\U0001F1FF"  # more flags
                               u"\U00002702-\U000027B0"  # Dingbats
                               u"\U000024C2-\U0001F251"  # enclosed characters
                               u"\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
                               u"\U0001f926-\U0001f937"  # additional emoticons
                               u"\U0001F620"  # angry face
                               u"\u200d"  # zero width joiner
                               u"\u2640-\u2642"  # gender symbols
                               "]+", flags=re.UNICODE)

    # Remove emojis and other special characters
    cleaned_text = emoji_pattern.sub(r'', text)

    # Remove any remaining non-printable characters
    cleaned_text = ''.join(char for char in cleaned_text if char.isprintable())

    return cleaned_text.strip()
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

async def get_verified_proxy(proxies):
    while True:
        proxy = random.choice(proxies)
        if await verify_proxy(proxy):
            return proxy
        print(f"Proxy {proxy['ip']}:{proxy['port']} failed verification, trying another...")
def get_sleep_interval(interval_type):
    if interval_type == "tiny":
        return random.uniform(0.5, 1.5)
    elif interval_type == "short":
        return random.uniform(2.5, 3.5)
    elif interval_type == "medium":
        return random.uniform(3, 5)
    elif interval_type == "long":
        return random.uniform(5, 8)
    else:
        return 2  # Default fallback
def get_state_abbreviations():
    return {
        "alabama": "AL",
        "alaska": "AK",
        "arizona": "AZ",
        "arkansas": "AR",
        "california": "CA",
        "colorado": "CO",
        "connecticut": "CT",
        "delaware": "DE",
        "florida": "FL",
        "georgia": "GA",
        "hawaii": "HI",
        "idaho": "ID",
        "illinois": "IL",
        "indiana": "IN",
        "iowa": "IA",
        "kansas": "KS",
        "kentucky": "KY",
        "louisiana": "LA",
        "maine": "ME",
        "maryland": "MD",
        "massachusetts": "MA",
        "michigan": "MI",
        "minnesota": "MN",
        "mississippi": "MS",
        "missouri": "MO",
        "montana": "MT",
        "nebraska": "NE",
        "nevada": "NV",
        "new hampshire": "NH",
        "new jersey": "NJ",
        "new mexico": "NM",
        "new york": "NY",
        "north carolina": "NC",
        "north dakota": "ND",
        "ohio": "OH",
        "oklahoma": "OK",
        "oregon": "OR",
        "pennsylvania": "PA",
        "rhode island": "RI",
        "south carolina": "SC",
        "south dakota": "SD",
        "tennessee": "TN",
        "texas": "TX",
        "utah": "UT",
        "vermont": "VT",
        "virginia": "VA",
        "washington": "WA",
        "west virginia": "WV",
        "wisconsin": "WI",
        "wyoming": "WY",
        "district of columbia": "DC",
        "american samoa": "AS",
        "guam": "GU",
        "northern mariana islands": "MP",
        "puerto rico": "PR",
        "united states virgin islands": "VI",
    }

def parse_proxy_file(file_path):
    proxies = []
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split(':')
            if len(parts) == 4:
                proxy = {
                    'ip': parts[0],
                    'port': parts[1],
                    'username': parts[2],
                    'password': parts[3]
                }
                proxies.append(proxy)
    return proxies

def get_random_proxy(proxies):
    return random.choice(proxies)