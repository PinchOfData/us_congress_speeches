import os
import logging

import pandas as pd
from data_processor import process_legislators_data
from data_retrieval import fetch_current_legislators, fetch_historical_legislators

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s"
)
logger = logging.getLogger()

# Fetch current and historical legislators
current_legislators = fetch_current_legislators()
historical_legislators = fetch_historical_legislators()

# Define the range of congress numbers to process
congress_numbers = range(114, 119)  # This will include 114, 115, 116, 117, 118

# Dictionary to store the resulting DataFrames
legislators_by_congress = {}

# Process each congress number
for congress in congress_numbers:
    # Process current and historical legislators for this congress
    current_legislators_congress = process_legislators_data(current_legislators, [congress])
    historical_legislators_congress = process_legislators_data(historical_legislators, [congress])
    
    # Concatenate the data
    legislators_by_congress[congress] = pd.concat(
        [current_legislators_congress, historical_legislators_congress], axis=0
    )
    
    # Log the completion
    logger.info(f"Processed and concatenated {congress}th congress legislator data")

politician_info_folder = os.path.join(
    os.path.dirname(__file__), "..", "politician_info"
)
os.makedirs(politician_info_folder, exist_ok=True)

# Export dataframes to csv
for congress in legislators_by_congress.keys():
    fname = os.path.normpath(
        os.path.join(politician_info_folder, f"politician_info_{congress}th.csv")
    )
    legislators_by_congress[congress].to_csv(fname, index=False)
    logger.info(f"Saved {congress}th congress legislator data: {fname}")
  