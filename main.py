import logging
import os

import pandas as pd
from dotenv import load_dotenv

from match_politicians_info import CongressionalSpeechMatcher
from speeches_scraper import CongressionalRecordScraper, CongressionalSpeechExtractor

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s"
)
logger = logging.getLogger()


preloaded_speeches_path = os.path.join(os.path.dirname(__file__), "pdf_contents_df.csv")#os.getenv("PRESCRAPED_SPEECHES_PATH", "")
if preloaded_speeches_path:
    # Override with full preloaded speeaches
    logger.info(f"Reading preloaded speeches dataframe: {preloaded_speeches_path}")
    pdf_contents_df = pd.read_csv(preloaded_speeches_path, dtype=str)
else:
    scraper = CongressionalRecordScraper(api_key=os.environ["CONGRESS_API_KEY"])
    output_path = os.path.join(os.path.dirname(__file__), "pdf_contents_df.csv")
    pdf_contents_df = scraper.scrape_pdfs(output_path=output_path)

logger.info("Extracting speeches")
extractor = CongressionalSpeechExtractor(pdf_contents_df)
speeches_by_congress = extractor.extract_speeches()


# Load politician infos from files
logger.info("Reading politician info files")
politician_info_path = os.path.join(os.path.dirname(__file__), "politician_info")
csv_files = [f for f in os.listdir(politician_info_path) if f.endswith(".csv")]
# Dictionary to store DataFrames
poli_info_dataframes = {}
# Read each CSV file into a DataFrame
for csv_file in csv_files:
    file_path = os.path.join(politician_info_path, csv_file)
    df_name = csv_file.split("_")[2].replace("th", "").replace(".csv", "")
    df_name = int(df_name)
    poli_info_dataframes[df_name] = pd.read_csv(file_path).astype(str)

# Finally, match all the speeches with the policition info, congress by congress
congresses_to_match = [114, 115, 116, 117, 118]
final_dfs = {}
for congress in congresses_to_match:
    if congress not in speeches_by_congress or congress not in poli_info_dataframes:
        logging.warning(
            f"Data for Congress {congress} is missing in one of the datasets. Skipping..."
        )
        continue  # Skip to the next congress if data is missing
    logger.info(f"Matching speeches for congress {congress}")
    # Initialize the processor with the data for the given congress
    processor = CongressionalSpeechMatcher(
        speeches_by_congress[congress], poli_info_dataframes[congress]
    )

    # Match speakers and create the final data
    final_dfs[congress] = processor.match_speakers()

    logging.info(f"Successfully matched congress {congress}")

# Directory to save the file
output_directory = os.path.join(os.path.dirname(__file__), "matched_speeches")
os.makedirs(output_directory, exist_ok=True)

# Concatenate all DataFrames from final_dfs
concatenated_df = pd.concat(final_dfs.values(), ignore_index=True)

# Define the file path to save the concatenated DataFrame
file_path = os.path.join(output_directory, "speeches_with_info_114th_to_118th.csv")

concatenated_df.to_csv(file_path, index=False)
logging.info(f"Saved all matched speeches to {file_path}")
