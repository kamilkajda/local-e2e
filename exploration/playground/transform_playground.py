import json

import pandas as pd
import requests


def transfromation_playground():
    """
    A playground for transforming fetched GUS data into a Pandas DataFrame.
    """
    url = "https://bdl.stat.gov.pl/api/v1/data/by-variable/64428"
    params = {
        "unit-level": 2,  # Voivodships
        "page-size": 100,  # Fetch maximum items per page
        "format": "json",
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    # print(response.headers)
    # print(response.text)
    data = response.json()

    normalized_data = pd.json_normalize(data.get("results", []))
    df = pd.DataFrame(normalized_data)
    df

    print(f"Status: {response.status_code}")
    print(normalized_data.head())


if __name__ == "__main__":
    transfromation_playground()
