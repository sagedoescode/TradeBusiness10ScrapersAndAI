import os
import re
import pandas as pd
import numpy as np

import glob
from functions.get_geo_data import get_data_zipcoded


def read_csv_with_encoding(file_path):
    encodings = ['utf-8', 'iso-8859-1', 'cp1252']
    for encoding in encodings:
        try:
            # Read all columns as strings
            return pd.read_csv(file_path, encoding=encoding, dtype=str)
        except UnicodeDecodeError:
            continue
        except pd.errors.EmptyDataError:
            print(f"Warning: {file_path} is empty.")
            return pd.DataFrame()  # Return an empty DataFrame
        except Exception as e:
            print(f"Error reading {file_path}: {str(e)}")
            continue
    raise ValueError(f"Unable to read {file_path} with any of the attempted encodings")
def safe_split(value):
    if pd.isna(value) or not isinstance(value, str):
        return "", ""
    parts = value.split(maxsplit=1)
    return parts[0], parts[1] if len(parts) > 1 else ""





def merge_complementary_rows(state, niche):
    base_path = r'C:\Users\Sage\PycharmProjects\MasterScraper\data'
    input_file = os.path.join(base_path, rf'{state}_{niche}_central_stage_5.csv')
    # Read the CSV file
    df = pd.read_csv(input_file)

    # Function to check if two values are compatible (either same or one is empty)
    def are_compatible(val1, val2):
        return pd.isna(val1) or pd.isna(val2) or val1 == val2

    # Function to merge two rows
    def merge_rows(row1, row2):
        merged = row1.copy()
        for col in row1.index:
            if pd.isna(row1[col]) and not pd.isna(row2[col]):
                merged[col] = row2[col]
        return merged

    # Initialize a list to store merged rows
    merged_rows = []
    processed_indices = set()

    # Iterate through each row
    for i, row in df.iterrows():
        if i in processed_indices:
            continue

        merged = row.copy()
        merged_with_any = False

        # Compare with all other rows
        for j, other_row in df.iloc[i + 1:].iterrows():
            if j in processed_indices:
                continue

            # Check if rows are compatible
            compatible = True
            for col in df.columns:
                if not are_compatible(row[col], other_row[col]):
                    compatible = False
                    break

            if compatible:
                merged = merge_rows(merged, other_row)
                processed_indices.add(j)
                merged_with_any = True

        merged_rows.append(merged)
        processed_indices.add(i)


    output_file = os.path.join(base_path, rf'{state}_{niche}_central_all.csv')
    # Create a new DataFrame from merged rows
    result_df = pd.DataFrame(merged_rows, columns=df.columns)

    # Save the result to a new CSV file
    result_df.to_csv(output_file, index=False)
    print(f"Merged data saved to {output_file}")


def merge_csv_files(state, niche, stage):
    base_path = r'C:\Users\Sage\PycharmProjects\MasterScraper\data'

    file_patterns = {
        1: [
            f'df_{state}_{niche}_thumbtack.csv',
            f'yelp_{state}_{niche}.csv',
            f'scraped_data_bbb_{niche}_{state}.csv',
            f'consolidated_{state}_{niche}_businesses.csv'
        ],
        2: [
            f'updated_ma_sec_{state}_{niche}_businesses.csv',
            f'updated_{state}_{niche}_businesses_with_socials_OPENCORPS_with_owners.csv'
        ],
        3: [
            f'updated_{state}_{niche}_businesses_with_socials.csv'
        ],
        4: [
            f'updated_{state}_{niche}_businesses_with_heritage.csv'
        ],
        5: [
            f'updated_{state}_{niche}_businesses_with_owner_email.csv',
            fr'scraped_phone_numbers_{state}_{niche}.csv',
            f'{state}_{niche}_scraped_emails.csv'
        ]
    }

    dfs = []

    for pattern in file_patterns[stage]:
        file_path = os.path.join(base_path, pattern)
        if os.path.exists(file_path):
            try:
                df = read_csv_with_encoding(file_path)
                if df.empty:
                    print(f"Skipping empty file: {file_path}")
                    continue
                print(f"Successfully read {file_path}")

                if 'thumbtack' in pattern.lower():
                    print("Processing Thumbtack data")
                    df = process_thumbtack_data(df)
                elif 'yelp' in pattern.lower():
                    print("Processing Yelp data")
                    df = process_yelp_data(df, state)
                elif 'bbb' in pattern.lower():
                    print("Processing BBB data")
                    df = process_bbb_data(df)
                elif 'consolidated' in pattern.lower():
                    print("Processing Consolidated Maps data")
                    df = process_maps_data(df)
                elif 'ma_sec' in pattern.lower():
                    print("Processing MA SEC data")
                    df = process_ma_sec_data(df)
                elif 'opencorps' in pattern.lower():
                    print("Processing OpenCorps data")
                    df = process_opencorps_data(df)
                elif 'with_socials' in pattern.lower():
                    print("Processing Google Search data with socials")
                    df = process_google_search_data(df)
                elif 'heritage' in pattern.lower():
                    print("Processing Heritage data")
                    df = process_heritage_data(df)
                elif 'with_owner_email' in pattern.lower():
                    print("Processing Owner Email data")
                    df = process_owner_email_data(df)
                elif 'scraped_phone_numbers' in pattern.lower():
                    print("Processing Scraped Phone Numbers data")
                    df = process_scraped_phone_data(df, state, niche)
                elif 'scraped_emails' in pattern.lower():
                    print("Processing Scraped Emails data")
                    df = process_scraped_email_data(df)
                dfs.append(df)
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")
        else:
            print(f"File not found: {file_path}")

    if not dfs:
        print(f"No valid files found for stage {stage}")
        return

    merged_df = pd.concat(dfs, ignore_index=True)
    central_columns = [
        'Category', 'Language', 'Business Name', 'Phone #', 'Phone # 2',
        'Website', 'Site Rating', 'Reviews', 'Rating', 'Owner First Name',
        'Owner Last Name', 'Owners Cel #', 'Owners Phone #', 'Personal Email',
        'Business Email', 'Owner Social Media', 'Owner Social Media 2',
        'Instagram', 'Facebook', 'Linkedin', 'Business Address', 'City',
        'County', 'State', 'Google Link', 'Plus Code', 'Source'
    ]

    for col in central_columns:
        if col not in merged_df.columns:
            merged_df[col] = pd.NA

    merged_df = merged_df[central_columns]

    output_file = os.path.join(base_path, rf'{state}_{niche}_central_stage_{stage}.csv')
    merged_df.to_csv(output_file, index=False)

    print(f"Merged data for stage {stage} saved to {output_file}")


def process_thumbtack_data(df):
    def split_name(name):
        parts = name.split(maxsplit=1)
        return parts[0], parts[1] if len(parts) > 1 else ""

    df['city'], df['county'], df['state'] = zip(*df['Zip Code'].apply(lambda x: get_data_zipcoded(x, extract_zip=True)))
    df['Owner First Name'], df['Owner Last Name'] = zip(*df['Credentials_Name'].apply(safe_split))

    return pd.DataFrame({
        'Category': df['Niche'],
        'Language': pd.NA,
        'Business Name': df['Name'],
        'Phone #': df['Phone'],
        'Phone # 2': pd.NA,
        'Website': pd.NA,
        'Site Rating': pd.NA,
        'Reviews': pd.NA,
        'Rating': pd.NA,
        'Owner First Name': df['Owner First Name'],
        'Owner Last Name': df['Owner Last Name'],
        'Owners Cel #': pd.NA,
        'Owners Phone #': pd.NA,
        'Personal Email': pd.NA,
        'Business Email': df['Email'],
        'Owner Social Media': pd.NA,
        'Owner Social Media 2': pd.NA,
        'Instagram': df['Instagram'],
        'Facebook': df['Facebook'],
        'Linkedin': pd.NA,
        'Business Address': df['Zip Code'],  # Using Zip Code as address for now
        'City': df['city'],
        'County': df['county'],
        'State': df['state'],
        'Google Link': pd.NA,
        'Plus Code': pd.NA,
        'Source': 'Thumbtack'
    })

def process_yelp_data(df, state):
    def safe_zip_lookup(zip_code):
        if pd.isna(zip_code):
            return pd.NA, pd.NA, pd.NA
        try:

            return get_data_zipcoded(str(zip_code), extract_zip=True)
        except:
            return pd.NA, pd.NA, pd.NA

    df['city'], df['county'], df['state'] = zip(*df['location_zip_code'].apply(safe_zip_lookup))

    return pd.DataFrame({
        'Category': df['categories'].fillna('').astype(str),
        'Language': pd.NA,
        'Business Name': df['name'].fillna('').astype(str),
        'Phone #': df['display_phone'].fillna('').astype(str),
        'Phone # 2': pd.NA,
        'Website': df['business_url'].fillna('').astype(str),
        'Site Rating': pd.NA,
        'Reviews': df['review_count'].fillna(pd.NA).astype('Int64'),
        'Rating': df['rating'].fillna(pd.NA).astype(float),
        'Owner First Name': pd.NA,
        'Owner Last Name': pd.NA,
        'Owners Cel #': pd.NA,
        'Owners Phone #': pd.NA,
        'Personal Email': pd.NA,
        'Business Email': pd.NA,
        'Owner Social Media': pd.NA,
        'Owner Social Media 2': pd.NA,
        'Instagram': pd.NA,
        'Facebook': pd.NA,
        'Linkedin': pd.NA,
        'Business Address': df['location_address1'].fillna('').astype(str),
        'City': df['city'].fillna('').astype(str),
        'County': df['county'].fillna('').astype(str),
        'State': df['state'].fillna('').astype(str),
        'Google Link': pd.NA,
        'Plus Code': pd.NA,
        'Source': 'Yelp'
    })
def process_bbb_data(df):
    def extract_social_media(url):
        if pd.isna(url) or not isinstance(url, str):
            return pd.NA, pd.NA, pd.NA
        if 'facebook' in url.lower():
            return url, pd.NA, pd.NA
        elif 'instagram' in url.lower():
            return pd.NA, url, pd.NA
        elif 'linkedin' in url.lower():
            return pd.NA, pd.NA, url
        else:
            return pd.NA, pd.NA, pd.NA

    def extract_zip_and_get_location(address):
        if pd.isna(address) or not isinstance(address, str):
            return pd.NA, pd.NA, pd.NA
        zip_code_match = re.search(r'\b\d{5}\b', address)
        if zip_code_match:
            zip_code = zip_code_match.group()
            return get_data_zipcoded(zip_code, extract_zip=True)
        return pd.NA, pd.NA, pd.NA

    df['Facebook'], df['Instagram'], df['LinkedIn'] = zip(*df['Social Media'].apply(extract_social_media))
    df['city'], df['county'], df['state'] = zip(*df['Business Address'].apply(extract_zip_and_get_location))

    return pd.DataFrame({
        'Category': df['Business Category'],
        'Language': pd.NA,
        'Business Name': df['Business Name'],
        'Phone #': df['Business Phone'],
        'Phone # 2': pd.NA,
        'Website': df['Website'],
        'Site Rating': pd.NA,
        'Reviews': pd.NA,
        'Rating': pd.NA,
        'Owner First Name': pd.NA,
        'Owner Last Name': pd.NA,
        'Owners Cel #': pd.NA,
        'Owners Phone #': df['Management Phone'],
        'Personal Email': pd.NA,
        'Business Email': pd.NA,
        'Owner Social Media': pd.NA,
        'Owner Social Media 2': pd.NA,
        'Instagram': df['Instagram'],
        'Facebook': df['Facebook'],
        'Linkedin': df['LinkedIn'],
        'Business Address': df['Business Address'],
        'City': df['city'],
        'County': df['county'],
        'State': df['state'],
        'Google Link': pd.NA,
        'Plus Code': pd.NA,
        'Source': 'BBB'
    })

def process_maps_data(df):
    def extract_zip_code(address):
        if pd.isna(address) or not isinstance(address, str):
            return None
        zip_code_match = re.search(r'\b\d{5}\b', address)
        return zip_code_match.group() if zip_code_match else None

    def get_location_data(address):
        zip_code = extract_zip_code(address)
        if zip_code:
            try:
                city, county, state = get_data_zipcoded(zip_code, extract_zip=True)
                return city, county, state
            except Exception as e:
                print(f"Error getting location data for {zip_code}: {str(e)}")
        return pd.NA, pd.NA, pd.NA

    df['city'], df['county'], df['state'] = zip(*df['address'].apply(get_location_data))

    return pd.DataFrame({
        'Category': df['category'],
        'Language': pd.NA,
        'Business Name': df['name'],
        'Phone #': df['phone'],
        'Phone # 2': pd.NA,
        'Website': df['website'],
        'Site Rating': pd.NA,
        'Reviews': df['reviews_count'],
        'Rating': df['rating'],
        'Owner First Name': pd.NA,
        'Owner Last Name': pd.NA,
        'Owners Cel #': pd.NA,
        'Owners Phone #': pd.NA,
        'Personal Email': pd.NA,
        'Business Email': pd.NA,
        'Owner Social Media': pd.NA,
        'Owner Social Media 2': pd.NA,
        'Instagram': pd.NA,
        'Facebook': pd.NA,
        'Linkedin': pd.NA,
        'Business Address': df['address'],
        'City': df['city'],
        'County': df['county'],
        'State': df['state'],
        'Google Link': pd.NA,
        'Plus Code': df['plus_code'],
        'Source': 'Google Maps'
    })


def process_ma_sec_data(df):
    # Check if 'Owner First Name.1' exists in the dataframe
    if 'Owner First Name.1' in df.columns:
        owner_first_name = df['Owner First Name'].fillna(df['Owner First Name.1'])
    else:
        owner_first_name = df['Owner First Name']

    # Do the same for 'Owner Last Name.1'
    if 'Owner Last Name.1' in df.columns:
        owner_last_name = df['Owner Last Name'].fillna(df['Owner Last Name.1'])
    else:
        owner_last_name = df['Owner Last Name']

    return pd.DataFrame({
        'Category': df['Category'],
        'Language': df['Language'],
        'Business Name': df['Business Name'],
        'Phone #': df['Phone #'],
        'Phone # 2': df['Phone # 2'],
        'Website': df['Website'],
        'Site Rating': df['Site Rating'],
        'Reviews': df['Reviews'],
        'Rating': df['Rating'],
        'Owner First Name': owner_first_name,
        'Owner Last Name': owner_last_name,
        'Owners Cel #': df['Owners Cel #'],
        'Owners Phone #': df['Owners Phone #'],
        'Personal Email': df['Personal Email'],
        'Business Email': df['Business Email'],
        'Owner Social Media': df['Owner Social Media'],
        'Owner Social Media 2': df['Owner Social Media 2'],
        'Instagram': df['Instagram'],
        'Facebook': df['Facebook'],
        'Linkedin': df['Linkedin'],
        'Business Address': df['Business Address'],
        'City': df['City'],
        'County': df['County'],
        'State': df['State'],
        'Google Link': df['Google Link'],
        'Plus Code': df['Plus Code'],
        'Source': df['Source'].fillna('MA SEC')
    })
def process_opencorps_data(df):
    return pd.DataFrame({
        'Category': df['Category'],
        'Language': df['Language'],
        'Business Name': df['Business Name'],
        'Phone #': df['Phone #'],
        'Phone # 2': df['Phone # 2'],
        'Website': df['Website'],
        'Site Rating': df['Site Rating'],
        'Reviews': df['Reviews'],
        'Rating': df['Rating'],
        'Owner First Name': df['Owner First Name'],
        'Owner Last Name': df['Owner Last Name'],
        'Owners Cel #': df['Owners Cel #'],
        'Owners Phone #': df['Owners Phone #'],
        'Personal Email': df['Personal Email'],
        'Business Email': df['Business Email'],
        'Owner Social Media': df['Owner Social Media'],
        'Owner Social Media 2': df['Owner Social Media 2'],
        'Instagram': df['Instagram'],
        'Facebook': df['Facebook'],
        'Linkedin': df['Linkedin'],
        'Business Address': df['Business Address'],
        'City': df['City'],
        'County': df['County'],
        'State': df['State'],
        'Google Link': df['Google Link'],
        'Plus Code': df['Plus Code'],
        'Source': df['Source'].fillna('OPENCORPS')
    })


def process_google_search_data(df):
    return pd.DataFrame({
        'Category': df['Category'],
        'Language': df['Language'],
        'Business Name': df['Business Name'],
        'Phone #': df['Phone #'],
        'Phone # 2': df['Phone # 2'],
        'Website': df['Website'],
        'Site Rating': df['Site Rating'],
        'Reviews': df['Reviews'],
        'Rating': df['Rating'],
        'Owner First Name': df['Owner First Name'],
        'Owner Last Name': df['Owner Last Name'],
        'Owners Cel #': df['Owners Cel #'],
        'Owners Phone #': df['Owners Phone #'],
        'Personal Email': df['Personal Email'],
        'Business Email': df['Business Email'],
        'Owner Social Media': df['Owner Social Media'],
        'Owner Social Media 2': df['Owner Social Media 2'],
        'Instagram': df['Instagram'],
        'Facebook': df['Facebook'],
        'Linkedin': df['Linkedin'],
        'Business Address': df['Business Address'],
        'City': df['City'],
        'County': df['County'],
        'State': df['State'],
        'Google Link': df['Google Link'],
        'Plus Code': df['Plus Code'],
        'Source': df['Source']
    })
def process_heritage_data(df):
    return pd.DataFrame({
        'Category': df['Category'],
        'Language': df['Language'],  # This should now contain the heritage information
        'Business Name': df['Business Name'],
        'Phone #': df['Phone #'],
        'Phone # 2': df['Phone # 2'],
        'Website': df['Website'],
        'Site Rating': df['Site Rating'],
        'Reviews': df['Reviews'],
        'Rating': df['Rating'],
        'Owner First Name': df['Owner First Name'],
        'Owner Last Name': df['Owner Last Name'],
        'Owners Cel #': df['Owners Cel #'],
        'Owners Phone #': df['Owners Phone #'],
        'Personal Email': df['Personal Email'],
        'Business Email': df['Business Email'],
        'Owner Social Media': df['Owner Social Media'],
        'Owner Social Media 2': df['Owner Social Media 2'],
        'Instagram': df['Instagram'],
        'Facebook': df['Facebook'],
        'Linkedin': df['Linkedin'],
        'Business Address': df['Business Address'],
        'City': df['City'],
        'County': df['County'],
        'State': df['State'],
        'Google Link': df['Google Link'],
        'Plus Code': df['Plus Code'],
        'Source': df['Source']
    })

def process_owner_email_data(df):
    return pd.DataFrame({
        'Category': df['Category'],
        'Language': df['Language'],
        'Business Name': df['Business Name'],
        'Phone #': df['Phone #'],
        'Phone # 2': df['Phone # 2'],
        'Website': df['Website'],
        'Site Rating': df['Site Rating'],
        'Reviews': df['Reviews'],
        'Rating': df['Rating'],
        'Owner First Name': df['Owner First Name'],
        'Owner Last Name': df['Owner Last Name'],
        'Owners Cel #': df['Owners Cel #'],
        'Owners Phone #': df['Owners Phone #'],
        'Personal Email': df['Personal Email'],
        'Business Email': df['Business Email'],
        'Owner Social Media': df['Owner Social Media'].fillna(df['LinkedIn']),
        'Owner Social Media 2': df['Owner Social Media 2'],
        'Instagram': df['Instagram'],
        'Facebook': df['Facebook'],
        'Linkedin': df['Linkedin'],  # Use 'LinkedIn' if 'Linkedin' is empty
        'Business Address': df['Business Address'],
        'City': df['City'],
        'County': df['County'],
        'State': df['State'],
        'Google Link': df['Google Link'],
        'Plus Code': df['Plus Code'],
        'Source': df['Source']
    })


def process_scraped_phone_data(df, state, niche):
    # Read the CSV data into a DataFrame
    phone_data_csv = fr'C:\Users\Sage\PycharmProjects\MasterScraper\data\scraped_phone_numbers_{state}_{niche}.csv'
    phone_df = pd.read_csv(phone_data_csv)

    # Append the phone_df to the main df
    df = pd.concat([df, phone_df], ignore_index=True)

    return df
def process_scraped_email_data(df):
    return pd.DataFrame({
        'Category': df['Category'],
        'Language': df['Language'],
        'Business Name': df['Business Name'],
        'Phone #': df['Phone #'],
        'Phone # 2': df['Phone # 2'],
        'Website': df['Website'],
        'Site Rating': df['Site Rating'],
        'Reviews': df['Reviews'],
        'Rating': df['Rating'],
        'Owner First Name': df['Owner First Name'],
        'Owner Last Name': df['Owner Last Name'],
        'Owners Cel #': df['Owners Cel #'],
        'Owners Phone #': df['Owners Phone #'],
        'Personal Email': pd.NA,  # Assuming personal email is not provided in this dataset
        'Business Email': df['Email'],
        'Owner Social Media': df['Owner Social Media'],
        'Owner Social Media 2': df['Owner Social Media 2'],
        'Instagram': df['Instagram'],
        'Facebook': df['Facebook'],
        'Linkedin': df['Linkedin'],
        'Business Address': df['Business Address'],
        'City': df['City'],
        'County': df['County'],
        'State': df['State'],
        'Google Link': df['Google Link'],
        'Plus Code': df['Plus Code'],
        'Source': df['Source']
    })
#merge_complementary_rows(state="Massachusetts", niche="Marble & Granite")

# After ma_sec or get_company_info
#merge_csv_files(state="Massachusetts", niche="Counter Top", stage=3)

# After social_google_search and get_heritage
# merge_csv_files(state, niche, 3)
