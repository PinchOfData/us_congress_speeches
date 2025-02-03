import requests
import pandas as pd
import fitz
import logging
import re
import tqdm

logger = logging.getLogger()


class CongressionalRecordScraper:
    """
    A comprehensive tool for scraping and processing Congressional Record data
    """

    def __init__(
        self, api_key, base_url="https://api.congress.gov/v3/daily-congressional-record"
    ):
        """
        Initialize the scraper with API credentials and base URL

        :param api_key: API key for Congress.gov
        :param base_url: Base URL for the Congressional Record API
        """
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {"X-API-Key": self.api_key}

    def scrape_pdfs(
        self,
        output_path=None,
        records_to_fetch=10000,
        batch_size=250,
        cutoff_date="2015-01-06",
    ):
        """
        Run the complete data collection and processing pipeline

        :param output_path: Optional path to save the final output
        :return: DataFrame with extracted speeches
        """

        logger.info("Fetching congressional records")
        batch_size = min(batch_size, records_to_fetch)
        # Step 1: Fetch Congressional Records
        records_df = self._fetch_congressional_records(records_to_fetch, batch_size)
        records_df = records_df[records_df["issueDate"] >= cutoff_date]

        # Step 2: Extract PDF URLs
        logger.info("Extracting pdf urls")
        pdf_urls_df = self._extract_pdf_urls(records_df)

        # Step 3: Download PDF Contents
        logger.info("Downloading pdf contents")
        pdf_contents_df = self._download_pdf_contents(pdf_urls_df)

        # Optional: Save to file
        if output_path:
            pdf_contents_df.to_csv(output_path, index=False)
            logger.info(f"Data saved to {output_path}")

        return pdf_contents_df

    def _fetch_congressional_records(self, records_to_fetch, batch_size):
        """
        Fetch daily Congressional records from the API

        :param records_to_fetch: Total number of records to retrieve
        :param batch_size: Number of records to fetch in each API call
        :param start_offset: Starting offset for API pagination
        :return: DataFrame with issue dates and URLs
        """
        extracted_data = []
        offset = 0

        while offset < records_to_fetch:
            params = {"limit": batch_size, "offset": offset}

            try:
                logger.info(f"Getting records, offset: {offset} ")
                response = requests.get(
                    self.base_url, headers=self.headers, params=params
                )
                response.raise_for_status()

                data = response.json()
                offset += batch_size

                if "dailyCongressionalRecord" in data:
                    num_records = len(list(data["dailyCongressionalRecord"]))
                    if num_records != batch_size:
                        logger.info(
                            f"Num records: {num_records}, batch size: {batch_size}"
                        )
                    for item in data["dailyCongressionalRecord"]:
                        issue_date = item.get("issueDate")
                        url = item.get("url")
                        extracted_data.append({"issueDate": issue_date, "url": url})
                        logger.info(f"Issue Date: {issue_date} | URL: {url}")
                else:
                    logger.warning("No records found in the response.")

            except requests.RequestException as e:
                logger.error(f"API Request failed: {e}")
                break

        df = pd.DataFrame(extracted_data)
        df["issueDate"] = pd.to_datetime(df["issueDate"])
        return df

    def _extract_pdf_urls(self, df):
        """
        Extract PDF URLs for each Congressional Record

        :param df: DataFrame with issue dates and record URLs
        :return: DataFrame with PDF URLs
        """
        extracted_pdf_urls = []
        for index, row in tqdm.tqdm(
            df.iterrows(),
            total=len(df),
            desc="Extracting pdf urls",
        ):
            try:
                response = requests.get(row["url"], headers=self.headers)
                response.raise_for_status()
                json_data = response.json()

                entire_issue = json_data["issue"]["fullIssue"]["entireIssue"]
                for item in entire_issue:
                    if item["type"] == "PDF":
                        extracted_pdf_urls.append(
                            {"issueDate": row["issueDate"], "pdf_url": item["url"]}
                        )

            except Exception as e:
                logger.error(f"Error processing URL {row['url']}: {e}")

        return pd.DataFrame(extracted_pdf_urls)

    def _download_pdf_contents(self, pdf_df):
        """
        Download and extract text from PDF URLs

        :param pdf_df: DataFrame with PDF URLs
        :return: DataFrame with PDF contents
        """
        content_list = []

        for index, row in tqdm.tqdm(
            pdf_df.iterrows(),
            total=len(pdf_df),
            desc="Downloading pdf contents",
        ):
            pdf_url = row["pdf_url"]

            try:
                response = requests.get(pdf_url, stream=True, timeout=10)

                if response.status_code == 200:
                    pdf_document = fitz.open(stream=response.content, filetype="pdf")
                    text_content = " ".join(page.get_text() for page in pdf_document)
                    content_list.append(text_content)
                else:
                    content_list.append(
                        f"Failed to download content: HTTP {response.status_code}"
                    )

            except Exception as e:
                content_list.append(f"Error processing PDF: {e}")

        pdf_df["content"] = content_list

        return pdf_df


class CongressionalSpeechExtractor:
    def __init__(self, pdf_df):
        """
        Initialize the PDFProcessing class with the DataFrame containing PDF content

        :param pdf_df: DataFrame with PDF contents (must include 'content', 'issueDate', 'pdf_url' columns)
        """
        self.pdf_df = pdf_df

    def extract_speeches(self):
        """
        Extract speeches from cleaned PDF content

        :param pdf_df: DataFrame with PDF contents
        :return: DataFrame with extracted speeches
        """
        prefix = r"(Mr\.|Ms\.|Mrs\.|Miss)"
        pattern = (
            rf"\b{prefix}\s+"
            r"([A-Z]+(?:\s[A-Z]+)*)\s*"
            r"(of [\w ]*)?\.?\s*"
            r"(?:Mr\.|Madam)\s*Speaker,\s*"
            r"(?P<speech>[\s\S]*?)"
            r"(?="
            r"\bThe SPEAKER pro tempore\b|"
            rf"{prefix}\s*Speaker,\s*I reserve the balance of my time|"
            rf"\b{prefix}\s+([A-Z]+(?:\s[A-Z]+)*)\s*(of [\w ]*)?\.?\s*(?:Mr\.|Madam)\s*Speaker|"
            r"<SPEAKER_BREAK>|"
            r"$|"
            r"(?:[A-Z]+\s+[A-Z]+\s+[A-Z]+\s+[A-Z]+\s+[A-Z]+(?:\s+[A-Z]+)*)"
            r")"
        )

        data = []
        for index, row in self.pdf_df.iterrows():
            cleaned_text = self._clean_text(row["content"])
            matches = re.findall(pattern, cleaned_text, re.DOTALL)

            for match in matches:
                speaker = (match[1] + " " + match[2]).strip()
                speech = match[3].strip()
                data.append(
                    {
                        "issueDate": row["issueDate"],
                        "pdf_url": row["pdf_url"],
                        "speaker": speaker,
                        "speech": speech,
                    }
                )

        self.df_speeches = pd.DataFrame(data)
        self.speeches_by_congress = self._assign_congress_numbers()

        return self.speeches_by_congress

    def _clean_text(self, text):
        """
        Clean and normalize the extracted text

        :param text: Raw text to clean
        :return: Cleaned text
        """
        cleaning_rules = [
            (r"\nf\s\n", " <SPEAKER_BREAK> "),
            (r"\s+", " "),
            (r"(?<![.!?])\n", " "),
            (r"(\w+)-\s+(\w+)", r"\1\2"),
            (r"Jkt \d{6} PO \d{5} Frm \d{5} Fmt \d{4} Sfmt \d{4}", ""),
            (r"E:\\CR\\FM\\[A-Z0-9.]+ [A-Z0-9]+", ""),
            (r"DMWilson on DSKJM0X7X2PROD with", ""),
            (r"VerDate \w+ \d{2} \d{4} \d{2}:\d{2} \w+ \d{2}, \d{4}", ""),
            (r"\b(CONGRESSIONAL|RECORD|HOUSE|DAILY|DIGEST)\b", ""),
            (r"Pdnted on recycled papfil", ""),
        ]

        for pattern, replacement in cleaning_rules:
            text = re.sub(pattern, replacement, str(text))

        return text.strip()

    def _assign_congress_numbers(self):
        """
        Assign congress number based on the issue date.
        """
        # Convert issue date
        self.df_speeches["issueDate"] = pd.to_datetime(
            self.df_speeches["issueDate"]
        ).dt.tz_convert(None)

        def assign_congress_number(date):
            if pd.isnull(date):
                return None
            if date < pd.Timestamp("2017-01-03"):
                return 114
            elif date < pd.Timestamp("2019-01-03"):
                return 115
            elif date < pd.Timestamp("2021-01-03"):
                return 116
            elif date < pd.Timestamp("2023-01-03"):
                return 117
            else:
                return 118

        self.df_speeches["congress_number"] = self.df_speeches["issueDate"].apply(
            assign_congress_number
        )

        # Group speeches by congress number
        speeches_by_congress = {
            congress: group
            for congress, group in self.df_speeches.groupby("congress_number")
        }
        return speeches_by_congress
