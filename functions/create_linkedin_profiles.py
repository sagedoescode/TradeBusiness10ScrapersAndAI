import json
import os

PROFILES_JSON_PATH = 'profiles_linkedin.json'
BASE_PROFILE_PATH = r'C:\Users\Sage\PycharmProjects\MasterScraper\profiles'


def load_profiles():
    if os.path.exists(PROFILES_JSON_PATH) and os.path.getsize(PROFILES_JSON_PATH) > 0:
        with open(PROFILES_JSON_PATH, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}


def save_profiles(profiles):
    with open(PROFILES_JSON_PATH, 'w') as f:
        json.dump(profiles, f, indent=4)


def get_next_profile_number(profiles):
    existing_numbers = set()
    for profile in profiles.values():
        if 'profile_number' in profile:
            existing_numbers.add(profile['profile_number'])

    next_number = 1
    while next_number in existing_numbers:
        next_number += 1
    return next_number


def add_or_update_profile(profiles, profile_name):
    if profile_name in profiles:
        print(f"Profile '{profile_name}' already exists.")
        confirmation = input("Do you want to update it? (yes/no): ")
        if confirmation.lower() != 'yes':
            print("Profile update cancelled.")
            return

    email = input(f"Enter email for {profile_name}: ")
    password = input(f"Enter password for {profile_name}: ")
    proxy = input(f"Enter proxy for {profile_name} (press Enter to skip): ")

    if profile_name not in profiles:
        profile_number = get_next_profile_number(profiles)
    else:
        profile_number = profiles[profile_name].get('profile_number', get_next_profile_number(profiles))

    profile_path = os.path.join(BASE_PROFILE_PATH, f"Profile {profile_number}")
    os.makedirs(profile_path, exist_ok=True)

    profiles[profile_name] = {
        "email": email,
        "password": password,
        "proxy": proxy if proxy else None,
        "profile_number": profile_number,
        "cookies": {}  # Initialize empty cookies dictionary
    }

    save_profiles(profiles)
    print(f"Profile '{profile_name}' has been {'updated' if profile_name in profiles else 'added'}.")
    print(f"Profile folder created: {profile_path}")


def main():
    profiles = load_profiles()

    while True:
        profile_name = input("Enter profile name (or 'q' to quit): ")
        if profile_name.lower() == 'q':
            break
        add_or_update_profile(profiles, profile_name)


if __name__ == "__main__":
    main()