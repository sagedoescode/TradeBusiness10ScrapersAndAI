import pandas as pd
from typing import Tuple, Optional
import re

def load_zipcode_data(zipcodes_path: str) -> pd.DataFrame:
    """
    Load zipcode data from CSV file, ensuring 'code' is treated as string.

    :param zipcodes_path: Path to the zipcodes CSV file
    :return: DataFrame with zipcode information
    """
    return pd.read_csv(zipcodes_path, dtype={'code': str})

def get_city_state_county_by_zipcode(zipcode: str):
    data = load_zipcode_data(r'C:\Users\Sage\PycharmProjects\MasterScraper\data\geodata\all_us_zipcodes.csv')
    """
    Get city, state, and county information for a given zipcode.

    :param zipcode: The zipcode to look up
    :param data: DataFrame containing zipcode data
    :return: A tuple containing (city, state, county) or (None, None, None) if not found
    """
    result = data[data['code'] == zipcode]
    if not result.empty:
        return result.iloc[0]['city'], result.iloc[0]['state'], result.iloc[0]['county']
    return None, None, None




def extract_zipcode(address: str) -> str:
    """
    Extract the zip code from an address string.

    :param address: The full address string
    :return: The extracted zip code or an empty string if not found
    """
    pattern = r'\b\d{5}\b(?![-\d])'  # Matches exactly 5 digits at a word boundary
    match = re.search(pattern, address)
    return match.group() if match else ""
# This block will only run if the script is executed directly

def get_data_zipcoded(address, extract_zip):
    if extract_zip == True:
        zipcode = extract_zipcode(address)
    else:
        zipcode = address
    city, state, county = get_city_state_county_by_zipcode(zipcode)
    if city and state and county:

        return city, county, state
    else:

        return "", "", ""
if __name__ == "__main__":
    # Example usage
    address = "1 Wellesley Rd, Natick, MA 01760"
    get_data_zipcoded(address)


