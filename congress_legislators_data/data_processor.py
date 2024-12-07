import pandas as pd

from utilities import calculate_congresses_served

# Dictionary of state abbreviations to lowercase full state names
STATE_DICT = {
    "AL": "alabama",
    "AK": "alaska",
    "AZ": "arizona",
    "AR": "arkansas",
    "CA": "california",
    "CO": "colorado",
    "CT": "connecticut",
    "DE": "delaware",
    "FL": "florida",
    "GA": "georgia",
    "HI": "hawaii",
    "ID": "idaho",
    "IL": "illinois",
    "IN": "indiana",
    "IA": "iowa",
    "KS": "kansas",
    "KY": "kentucky",
    "LA": "louisiana",
    "ME": "maine",
    "MD": "maryland",
    "MA": "massachusetts",
    "MI": "michigan",
    "MN": "minnesota",
    "MS": "mississippi",
    "MO": "missouri",
    "MT": "montana",
    "NE": "nebraska",
    "NV": "nevada",
    "NH": "new hampshire",
    "NJ": "new jersey",
    "NM": "new mexico",
    "NY": "new york",
    "NC": "north carolina",
    "ND": "north dakota",
    "OH": "ohio",
    "OK": "oklahoma",
    "OR": "oregon",
    "PA": "pennsylvania",
    "RI": "rhode island",
    "SC": "south carolina",
    "SD": "south dakota",
    "TN": "tennessee",
    "TX": "texas",
    "UT": "utah",
    "VT": "vermont",
    "VA": "virginia",
    "WA": "washington",
    "WV": "west virginia",
    "WI": "wisconsin",
    "WY": "wyoming",
}


def process_legislators_data(data, target_congress_numbers=None):
    """
    Process legislator data and extract relevant information.

    Args:
        data (list): List of legislator records
        target_congress_numbers (list, optional): List of Congress numbers to filter by

    Returns:
        pd.DataFrame: Processed legislator information
    """
    result = []
    for legislator in data:
        name = legislator["name"]
        terms = legislator["terms"]
        bio = legislator["bio"]

        # Get the two most recent terms or process all terms
        terms_to_process = terms[-2:] if len(terms) > 2 else terms

        for term in terms_to_process:
            # Calculate Congress numbers for this term
            congress_numbers = calculate_congresses_served(term["start"], term["end"])

            # Filter by target Congress numbers if specified
            if target_congress_numbers and not any(
                num in target_congress_numbers for num in congress_numbers
            ):
                continue

            # Determine full name
            official_full = name.get("official_full", "N/A")
            if official_full == "N/A":
                official_full = f"{name['first']} {name['last']}"

            # Prepare legislator information
            polit_info = {
                "first_name": name["first"].lower(),
                "last_name": name["last"].lower(),
                "gender": bio.get("gender", "N/A"),
                "birthday": bio.get("birthday", "N/A"),
                "name": official_full,
                "type": term["type"],
                "party": term["party"],
                "start_date": term["start"],
                "end_date": term["end"],
                "congress_numbers": congress_numbers,
                "state": STATE_DICT.get(term["state"], term["state"].lower()),
            }

            result.append(polit_info)

    return pd.DataFrame(result)
