import logging

import requests
import yaml

logger = logging.getLogger()


def fetch_current_legislators():
    """
    Fetch current legislators data from the United States Congress API.

    Returns:
        list: Parsed YAML data of current legislators
    """
    url = "https://theunitedstates.io/congress-legislators/legislators-current.yaml"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = yaml.safe_load(response.content.decode("utf-8", "ignore"))
        logger.info("Successfully fetched current legislators")
        return data
    except requests.RequestException as e:
        logger.error(f"Error fetching current legislators: {e}")
        return []


def fetch_historical_legislators():
    """
    Fetch historical legislators data from the United States Congress API.

    Returns:
        list: Parsed JSON data of historical legislators
    """
    url = "https://theunitedstates.io/congress-legislators/legislators-historical.json"
    try:
        response = requests.get(url)
        response.raise_for_status()
        logger.info("Successfully fetched historical legislators")
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching historical legislators: {e}")
        return []
