import logging
import os

import pandas as pd
from dotenv import load_dotenv

from match_politicians_info import CongressionalSpeechMatcher
from speeches_scraper import CongressionalRecordScraper, CongressionalSpeechExtractor
from utilities import assign_congress_numbers

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s"
)
logger = logging.getLogger()


preloaded_speeches_path = os.getenv("PRESCRAPED_SPEECHES_PATH", "")
if preloaded_speeches_path:
    # Override with full preloaded speeaches
    logger.info("Reading preloaded speeches dataframe")
    preloaded_speeches = pd.read_csv(preloaded_speeches_path)
    speeches_by_congress = assign_congress_numbers(preloaded_speeches)

else:
    scraper = CongressionalRecordScraper(api_key=os.environ["CONGRESS_API_KEY"])
    pdf_contents_df = scraper.scrape_pdfs()

    extractor = CongressionalSpeechExtractor(pdf_contents_df)
    speeches_by_congress = extractor.extract_speeches()

# Load politician infos from files
logger.info("Reading politician info files")
folder_path = os.getenv("POLITICIAN_INFO_PATH")
csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
# Dictionary to store DataFrames
poli_info_dataframes = {}
# Read each CSV file into a DataFrame
for csv_file in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    df_name = csv_file.split("_")[2].replace("th", "").replace(".csv", "")
    df_name = int(df_name)
    poli_info_dataframes[df_name] = pd.read_csv(file_path).astype(str)

# FInally, match all the speeches with the policition info, congress by congress
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
