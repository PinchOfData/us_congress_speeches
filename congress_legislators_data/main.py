import os

import pandas as pd
from data_processor import process_legislators_data
from data_retrieval import fetch_current_legislators, fetch_historical_legislators

# Fetch current and historical legislators
current_legislators = fetch_current_legislators()
historical_legislators = fetch_historical_legislators()

# Create legislator info dataframe for congress 117
current_legislators_117 = process_legislators_data(current_legislators, [117])
historical_legislators_117 = process_legislators_data(historical_legislators, [117])
legislators_117 = pd.concat(
    [current_legislators_117, historical_legislators_117], axis=0
)

# Create legislator info dataframe for congress 118
legislators_118 = process_legislators_data(current_legislators, [118])
legislators_118 = legislators_118.drop_duplicates(subset=["name"], keep="first")

# Export dataframes to csv
legislators_117.to_csv(
    os.path.join("..", "politician_info", "politician_info_117th.csv"), index=False
)
legislators_118.to_csv(
    os.path.join("..", "politician_info", "politician_info_118th.csv"), index=False
)
