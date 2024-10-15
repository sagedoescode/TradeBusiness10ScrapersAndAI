from googleapiclient.discovery import build
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1ILnlUYCMQl1gdfqRQM0PtB-N2d5QDynBQ9Rdeewcq4U'

creds = service_account.Credentials.from_service_account_file(
    'creds/verdant-coyote-430500-j4-ef1fad885b23.json', scopes=SCOPES)

service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

state_coordinates = {
    "massachusetts": ("42.939151, -73.245314", "41.390332, -70.611790"),
    "connecticut": ("42.061205, -73.754797", "41.040772, -71.786885"),
    "new york": ("45.025036, -79.816910", "40.608932, -71.941418"),
    "new jersey": ("41.419830, -75.731682", "38.935659, -73.949026"),
    "pennsylvania": ("42.256642, -80.522705", "39.668787, -74.831787"),
    "florida": ("30.991533, -87.783180", "25.159986, -80.070777"),
    "texas": ("36.609612, -106.955401", "25.746727, -93.628894"),
    "california": ("42.002149, -124.377109", "32.738292, -114.406966"),
    "nevada": ("42.026170, -120.058124", "34.995500, -114.004658"),

}

def get_states():
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range='States!B2:B').execute()
    values = result.get('values', [])
    states_with_coords = []
    for item in values:
        if item:
            state = item[0].lower()
            coords = state_coordinates.get(state.lower(), (None, None))
            states_with_coords.append((state, coords[0], coords[1]))
    return states_with_coords

def get_niches():
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range='List By Priority!B2:E').execute()
    values = result.get('values', [])
    niches = []
    for row in values:
        if row:
            niches.extend([item.strip() for item in row if item.strip()])
    return niches



