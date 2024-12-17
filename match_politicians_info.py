import logging

import pandas as pd
from fuzzywuzzy import process
from unidecode import unidecode

logger = logging.getLogger()


class CongressionalSpeechMatcher:
    def __init__(self, df_speeches, df_politician_info):
        """
        Initialize the processor with DataFrames for speeches and politician information.
        :param df_speeches: DataFrame containing congressional speeches
        :param df_politician_info: DataFrame containing politician information
        """
        self.df_speeches = df_speeches.copy()
        self.df_politician_info = df_politician_info.copy()

    def match_speakers(self):
        """
        Match speakers using different matching strategies.

        :return: Combined DataFrame with matched speakers
        """

        logger.info("Processing speaker names")
        self._preprocess_speaker_names()

        logger.info("Processing politician info")
        self._preprocess_politician_info()

        logger.info("Matching speakers with poltician info")
        # Split speeches by matching criteria
        (
            df_all_three,
            df_state_lastname_only,
            df_lastname_only,
            df_firstname_lastname_only,
        ) = self._split_speeches_by_matching_criteria(self.df_speeches)

        # Match strategies
        datasets = []

        # 1. Match by first name, last name, and state
        df_matched_all_3 = self._match_speakers_with_criteria(
            df_all_three, ["state", "first_name", "last_name"]
        )
        datasets.append(df_matched_all_3)

        # 2. Match by state and last name
        df_matched_state_lastname_only = self._match_speakers_with_criteria(
            df_state_lastname_only, ["state", "last_name"]
        )
        datasets.append(df_matched_state_lastname_only)

        # 3. Match by last name only
        df_matched_lastname_only = self._match_speakers_with_criteria(
            df_lastname_only, ["last_name"]
        )
        datasets.append(df_matched_lastname_only)

        # 4. Match by fuzzy matching full name and state
        df_remaining = df_all_three[~df_all_three.index.isin(df_matched_all_3.index)]
        df_name_state = self._match_by_fuzzy_long_name(df_remaining)
        datasets.append(df_name_state)

        # 5. Match by fuzzy matching full name
        df_long_name = self._match_by_fuzzy_long_name(df_firstname_lastname_only)

        # Remove certain congress-specific patterns
        congress = df_firstname_lastname_only["congress_number"].unique()
        assert len(congress) == 1
        congress = congress[0]
        if congress == 115:
            remove_patterns = ["barraga n"]
        elif congress == 116:
            remove_patterns = ["luja n", "barraga n", "luga n"]
        elif congress == 117:
            remove_patterns = ["barraga n", "c rdenas"]
        elif congress == 118:
            remove_patterns = ["barraga n", "jackson lee", "cline member"]
        else:
            remove_patterns = []

        df_long_name_cleaned = self._cleanup_long_name_matches(
            df_long_name, remove_patterns
        )
        datasets.append(df_long_name_cleaned)

        # Combine all matched datasets
        combined_df = pd.concat(datasets, axis=0)

        # Remove duplicates and unnecessary columns
        combined_df = self._finalize_matched_dataset(combined_df)

        return combined_df

    def _preprocess_speaker_names(self):
        """
        Clean and extract information from speaker names.
        """
        # Convert speaker names to lowercase
        self.df_speeches["speaker"] = (
            self.df_speeches["speaker"].str.lower().apply(unidecode)
        )

        # Extract state from speaker name
        self.df_speeches["state"] = self.df_speeches["speaker"].apply(
            lambda x: x.split(" of ")[-1] if " of " in x else pd.NA
        )

        # Remove state from speaker name
        self.df_speeches["speaker"] = self.df_speeches["speaker"].apply(
            lambda x: x.split(" of ")[0] if " of " in x else x
        )

        # Split speaker names into first and last names
        for idx, name in self.df_speeches["speaker"].items():
            name_parts = name.split()
            self.df_speeches.at[idx, "first_name"] = (
                name_parts[0].strip() if len(name_parts) > 1 else pd.NA
            )
            self.df_speeches.at[idx, "last_name"] = name_parts[-1].strip()

    def _preprocess_politician_info(self):
        """
        Preprocess politician information.
        """

        # Preprocess names
        self.df_politician_info["name"] = self.df_politician_info["name"].str.lower()

        # Split names
        if "sort_name" in self.df_politician_info.columns:
            self.df_politician_info[["last_name", "first_name"]] = (
                self.df_politician_info["sort_name"].str.split(", ", expand=True)
            )

        self.df_politician_info[["last_name", "first_name"]] = self.df_politician_info[
            ["last_name", "first_name"]
        ].apply(lambda x: x.str.lower())

        # Remove accents from names
        self.df_politician_info["name"] = self.df_politician_info["name"].apply(
            unidecode
        )

        # Extract state from area
        if "area" in self.df_politician_info.columns:
            self.df_politician_info["state"] = (
                self.df_politician_info["area"].str.split("'s").str[0].str.lower()
            )

        # Drop columns with too many missing values
        self.df_politician_info.dropna(
            thresh=len(self.df_politician_info) * 0.1, axis=1, inplace=True
        )

        return self.df_politician_info

    def _split_speeches_by_matching_criteria(self, df):
        """
        Split speeches into different groups based on available information.
        :return: Tuple of DataFrames split by matching criteria
        """
        df["speech_id"] = df.index

        # Split into groups based on available information
        df_all_three = df[df[["state", "first_name", "last_name"]].notna().all(axis=1)]
        df_state_lastname_only = df[
            (df["state"].notna())
            & (df["last_name"].notna())
            & (df["first_name"].isna())
        ]
        df_lastname_only = df[
            (df["last_name"].notna()) & (df["first_name"].isna()) & (df["state"].isna())
        ]
        df_firstname_lastname_only = df[
            (df["first_name"].notna())
            & (df["last_name"].notna())
            & (df["state"].isna())
        ]

        return (
            df_all_three,
            df_state_lastname_only,
            df_lastname_only,
            df_firstname_lastname_only,
        )

    def _match_speakers_with_criteria(self, df, match_columns):
        """
        Match speakers based on specified columns.

        :param df: DataFrame to match
        :param match_columns: List of columns to match on (e.g., ["state", "last_name"])
        :return: Matched DataFrame
        """
        # Validate input
        if not isinstance(match_columns, list) or not all(
            isinstance(col, str) for col in match_columns
        ):
            logger.error(
                "Invalid input: match_columns must be a list of column names (strings)."
            )

        # Prepare dataframes for matching
        cols_to_drop = df.columns.intersection(self.df_politician_info.columns)
        cols_to_drop = [col for col in cols_to_drop if col not in match_columns]
        df_reset = df.drop(columns=cols_to_drop)

        # Get unique politician info
        df_politician_info_unique = self.df_politician_info.drop_duplicates(
            subset=match_columns, keep=False
        )

        # Perform matching
        df_matched = pd.merge(
            df_reset,
            df_politician_info_unique,
            on=match_columns,
            how="inner",
            validate="many_to_one",
        )

        # Add match metadata
        df_matched["matched_by"] = ", ".join(match_columns)
        df_matched["similarity_score"] = 100

        return df_matched

    def _match_by_fuzzy_long_name(self, df, threshold=50):
        """
        Match speakers using fuzzy matching on full name.

        :param df: DataFrame to match
        :param threshold: Similarity score threshold
        :return: Matched DataFrame
        """
        best_matches = []

        for speaker, state in zip(df["speaker"], df["state"]):
            # Filter for correct state if available
            df_correct_state = self.df_politician_info.copy()
            if pd.notna(state):
                df_correct_state = df_correct_state[df_correct_state["state"] == state]

            # Fuzzy matching
            closest_matches = process.extract(
                speaker, df_correct_state["name"].tolist(), limit=1
            )

            if not closest_matches:
                continue

            name, score = closest_matches[0]

            if score >= threshold:
                matched_row = df_correct_state[
                    df_correct_state["name"] == name
                ].squeeze()
                matched_row = matched_row.copy()
                matched_row["similarity_score"] = score
                if pd.notna(state):
                    matched_row["matched_by"] = "long_name, state"
                else:
                    matched_row["matched_by"] = "long_name"
                best_matches.append(matched_row)

        # Convert matches to DataFrame
        if not best_matches:
            return pd.DataFrame()

        df_best_matches = pd.DataFrame(best_matches)

        # Prepare for concatenation
        cols_to_drop = df.columns.intersection(df_best_matches.columns)
        df_reset = df.reset_index(drop=True).drop(columns=cols_to_drop)
        df_best_matches_reset = df_best_matches.reset_index(drop=True)

        # Combine DataFrames
        df_matched = pd.concat([df_reset, df_best_matches_reset], axis=1)

        return df_matched

    def _cleanup_long_name_matches(self, df, remove_patterns):
        """
        Clean up long name matches by removing problematic entries.

        :param df: DataFrame to clean
        :param remove_patterns: List of problematic name patterns to remove.
        :return: Cleaned DataFrame
        """
        # Ensure remove_patterns is a list
        if not isinstance(remove_patterns, list):
            raise ValueError("remove_patterns should be a list of strings.")

        # Filter rows by checking if problematic patterns appear in the 'name' column
        for pattern in remove_patterns:
            df = df[~df["speaker"].str.contains(pattern, case=False, na=False)]

        return df

    def _finalize_matched_dataset(self, combined_df):
        """
        Finalize the matched dataset by removing duplicates and unnecessary columns.

        :param combined_df: Combined DataFrame to finalize
        :return: Finalized DataFrame
        """
        # Remove duplicates
        combined_df = combined_df.drop_duplicates()

        # Remove unnecessary columns if they exist
        columns_to_drop = ["term"] if "term" in combined_df.columns else []
        combined_df = combined_df.drop(columns=columns_to_drop, errors="ignore")

        return combined_df
