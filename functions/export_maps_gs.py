import csv
import time
import requests
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
import ssl
from socket import timeout as socket_timeout
from functions.useful import get_state_abbreviations
from functions.get_geo_data import get_data_zipcoded


def create_state_sheet_if_not_exists(sheet, SPREADSHEET_ID, state):
    sheet_metadata = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
    sheets = sheet_metadata.get('sheets', '')
    sheet_names = [s['properties']['title'] for s in sheets]
    state = state.capitalize()
    if state not in sheet_names:
        state_sheet_id = next((s['properties']['sheetId'] for s in sheets if s['properties']['title'] == 'State'), None)
        if state_sheet_id is not None:
            request = {'destinationSpreadsheetId': SPREADSHEET_ID}
            response = sheet.sheets().copyTo(spreadsheetId=SPREADSHEET_ID, sheetId=state_sheet_id,
                                             body=request).execute()
            requests = [{
                'updateSheetProperties': {
                    'properties': {
                        'sheetId': response['sheetId'],
                        'title': state,
                        'hidden': False
                    },
                    'fields': 'title,hidden'
                }
            }]
            body = {'requests': requests}
            sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
            print(f"Created new sheet for {state}")
        else:
            print("Error: 'State Sheet' not found")
    else:
        print(f"Sheet for {state} already exists")


def export(niche, state, max_retries=5, initial_retry_delay=5, max_retry_delay=60):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    SPREADSHEET_ID = '1ILnlUYCMQl1gdfqRQM0PtB-N2d5QDynBQ9Rdeewcq4U'
    creds = service_account.Credentials.from_service_account_file(
        'creds/verdant-coyote-430500-j4-ef1fad885b23.json', scopes=SCOPES)
    state_abbreviations = get_state_abbreviations()
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    create_state_sheet_if_not_exists(sheet, SPREADSHEET_ID, state)

    # Get existing data
    existing_data = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=f'{state}!A:C').execute().get('values', [])
    existing_business_names = set(row[2] for row in existing_data if len(row) > 2)

    COLUMN_MAPPING = {
        0: None,  # Niche (comes from variable, not CSV)
        1: 1,  # Language
        2: 2,  # Business Name
        3: 0,  # Business Category
        4: 3,  # Phone #
        5: 5,  # Website
        6: 6,  # Site Rating
        7: 7,  # Reviews
        8: 8,  # Rating
        9: 9,  # Owner First Name
        10: 10,  # Owner Last Name
        11: 11,  # Owners Cel #
        12: 12,  # Owners Phone #
        13: 13,  # Personal Email
        14: 14,  # Business Email
        15: 15,  # Owner Social Media
        16: 16,  # Owner Social Media
        17: 17,  # Instagram
        18: 19,  # Linkedin
        19: 18,  # Facebook
        20: 20,  # Business Address
        21: 21,  # City
        22: 22,  # County
        23: 23,  # State
        24: 24,  # Google Link
        25: 25,  # Plus Code
        26: 26,  # Source
    }

    with open(fr'C:\Users\Sage\PycharmProjects\MasterScraper\data\{state}_{niche}_CENTRAL_all.csv',
              'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        csv_data = list(csv_reader)[1:]  # Skip the header row

    max_sheet_index = max(COLUMN_MAPPING.keys())
    max_csv_index = max(csv_index for csv_index in COLUMN_MAPPING.values() if csv_index is not None)
    print(max_sheet_index)
    print(max_csv_index)
    new_data = []

    for row in csv_data:

        if not any(row):

            continue

        padded_row = row + [''] * (max_csv_index + 1 - len(row))
        new_row = [''] * (max_sheet_index + 1)
        for sheet_index, csv_index in COLUMN_MAPPING.items():

            if csv_index is not None:

                new_row[sheet_index] = padded_row[csv_index]
        new_row = ['' if cell == 'N/A' else cell for cell in new_row]

        if new_row[2] in existing_business_names:

            continue

        new_row[0] = niche


        print(f"Adding new row: {new_row}")
        if 'shop' in new_row[3].lower():
            print(new_row[3])
            continue
        new_data.append(new_row)
    print(f"NEW DATA {new_data}")
    # Sort rows by the number of non-empty cells (descending)
    new_data.sort(key=lambda row: sum(1 for cell in row if cell), reverse=True)
    print(f"NEW DATA {new_data}")
    if new_data:

        RANGE_NAME = f'{state}!A{len(existing_data) + 1}'
        batch_update_values_request_body = {
            'value_input_option': 'RAW',
            'data': [
                {
                    'range': RANGE_NAME,
                    'values': new_data
                }
            ]
        }

        retry_delay = initial_retry_delay
        for attempt in range(max_retries):
            try:
                request = sheet.values().batchUpdate(spreadsheetId=SPREADSHEET_ID,
                                                     body=batch_update_values_request_body)
                response = request.execute()

                total_updated_cells = sum(update['updatedCells'] for update in response.get('responses', []))
                print(f"{total_updated_cells} cells updated.")
                break
            except HttpError as e:
                if e.resp.status in [403, 429]:
                    print(f"Rate limit reached. Waiting {retry_delay} seconds before retrying...")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, max_retry_delay)
                else:
                    print(f"HTTP Error occurred: {e}")
                    raise
            except (ConnectionAbortedError, ssl.SSLEOFError, socket_timeout) as e:
                print(
                    f"Connection error: {e}. Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)
            except Exception as e:
                print(f"Unexpected error occurred: {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, max_retry_delay)
                else:
                    raise
        else:
            print(f"Failed to update data after {max_retries} attempts.")
    else:
        print("No new data to update.")

    print("Data export completed.")


if __name__ == '__main__':
    export(niche="Marble & Granite", state="massachusetts")