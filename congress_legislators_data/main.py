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

# Create legislator info dataframe for congress 117
current_legislators_117 = process_legislators_data(current_legislators, [117])
historical_legislators_117 = process_legislators_data(historical_legislators, [117])
legislators_117 = pd.concat(
    [current_legislators_117, historical_legislators_117], axis=0
)
logger.info("Processed and concatenated 117th congress legislator data")
# Create legislator info dataframe for congress 118
legislators_118 = process_legislators_data(current_legislators, [118])
legislators_118 = legislators_118.drop_duplicates(subset=["name"], keep="first")
logger.info("Processed and concatenated 118th congress legislator data")


politicain_info_folder = os.path.join(
    os.path.dirname(__file__), "..", "politician_info"
)
# Export dataframes to csv
fname_117 = os.path.normpath(
    os.path.join(politicain_info_folder, "politician_info_117th.csv")
)
legislators_117.to_csv(fname_117, index=False)
logger.info(f"Saved 117th congress legislator data: {fname_117}")

fname_118 = os.path.normpath(
    os.path.join(politicain_info_folder, "politician_info_118th.csv")
)
legislators_118.to_csv(fname_118, index=False)
logger.info(f"Saved 118th congress legislator data: {fname_118}")
