import textwrap
import pandas as pd


def printwrap(text, width=150):
    """
    Function to print wrapped text to the console for better readability.
    """
    print(textwrap.fill(text, width=width))


def assign_congress_numbers(df_speeches):
    """
    Assign congress number based on the issue date.
    """
    # Convert issue date
    df_speeches["issueDate"] = pd.to_datetime(df_speeches["issueDate"]).dt.tz_convert(
        None
    )
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

    df_speeches["congress_number"] = df_speeches["issueDate"].apply(
        assign_congress_number
    )

    # Group speeches by congress number
    df_speeches_by_congress = {
        congress: group for congress, group in df_speeches.groupby("congress_number")
    }
    return df_speeches_by_congress
