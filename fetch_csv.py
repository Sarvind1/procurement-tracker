import requests
import csv
import json
import sys
from io import StringIO
import pandas as pd
import os

# Power Automate webhook URL
url = "https://default0922decaaf3c4870acea84b9557b04.6a.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/44beb61d9191460db68d59f67b7c87b3/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=y72LE0O_3WwR5RidMhN4MM6ZrHdegK6wayOf7Vt2EIw"

def fetch_csv_data(list_name):
    """
    Fetch CSV data from the Power Automate webhook.

    Args:
        list_name: The SharePoint list name to fetch data from

    Returns the response content.
    """
    try:
        # Prepare request body with list name
        payload = {"list_name": list_name}
        headers = {"Content-Type": "application/json"}

        # Make POST request to the URL
        response = requests.post(url, json=payload, headers=headers)

        # Check if request was successful
        response.raise_for_status()

        # Parse JSON response
        json_response = response.json()

        # Extract CSV data from the JSON response
        if 'csv_data' in json_response:
            return json_response['csv_data']
        else:
            print("Warning: 'csv_data' key not found in response")
            return response.text

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None

def parse_csv(csv_content):
    """
    Parse CSV content and return as list of dictionaries.
    """
    if not csv_content:
        return []

    csv_reader = csv.DictReader(StringIO(csv_content))
    return list(csv_reader)

def main(list_name=None):
    print(f"Fetching CSV data from Power Automate for list: {list_name}...")

    # Fetch the CSV data
    csv_content = fetch_csv_data(list_name)

    if csv_content:
        print("\nCSV data received successfully!")

        # Parse and display the data
        data = parse_csv(csv_content)

        df = pd.read_csv(StringIO(csv_content))

        # Save to file
        os.makedirs("Pull", exist_ok=True)

        file_path = os.path.join("Pull", "output.csv")

        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            f.write(csv_content)

        print(f"\nList pull saved to '{file_path}'")

        return df

    else:
        print("Failed to fetch CSV data.")

if __name__ == "__main__":
    main(list_name="None")
