import json
import sys

import requests


def debug_api_response():
    """
    Step-by-step guide to inspecting API response structure using Python.
    Use this method when you encounter a new API and don't know its JSON layout.
    """

    # 1. Setup the request (Target: Wages variable 64428)
    url = "https://bdl.stat.gov.pl/api/v1/data/by-variable/64428"
    params = {
        "unit-level": 2,  # Voivodships
        "page-size": 1,  # Fetch only 1 item to keep the output clean!
        "format": "json",
    }

    print(f"--- 1. Sending Request ---")
    print(f"URL: {url}")

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()

        # 2. Get Raw JSON (Python Dictionary)
        # This converts the text response from server into a Python object (dicts and lists)
        data = response.json()

        print(f"\n--- 2. Top Level Keys ---")
        # This tells us what are the main "chapters" of the response
        print(f"Keys found: {list(data.keys())}")

        # 3. Inspecting 'results'
        # We see 'results' is a key, so let's see what's inside.
        print(f"\n--- 3. Inspecting 'results' content ---")
        results = data.get("results")

        # Check type - is it a List [] or a Dictionary {}?
        print(f"Type of 'results': {type(results)}")

        if isinstance(results, list) and len(results) > 0:
            print(f"Number of items in list: {len(results)}")

            # 4. Deep Dive into the FIRST item
            # We don't need to look at all 16 voivodships, just the first one to understand the schema.
            first_item = results[0]

            print(f"\n--- 4. Structure of a Single Record (First Item) ---")
            # json.dumps with indent=4 makes it pretty and readable for humans
            print(json.dumps(first_item, indent=4))

            print(f"\n--- 5. Analysis Conclusion ---")
            print(
                "Based on the output above (Step 4), to get the value '6000.50', you need to navigate:"
            )
            print("1. data['results'] -> gets the list")
            print("2. [0] -> gets the first unit")
            print("3. ['values'] -> gets the list of years")
            print("4. [0] -> gets the first year object")
            print("5. ['val'] -> gets the actual number")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    debug_api_response()
