from datetime import date, datetime
from typing import List, Union


def calculate_congresses_served(
    start_date: Union[str, date], end_date: Union[str, date]
) -> List[int]:
    """
    Calculate all Congress numbers a politician served in given their start and end dates.

    Args:
        start_date: Start date (YYYY-MM-DD format if string)
        end_date: End date (YYYY-MM-DD format if string)

    Returns:
        List of Congress numbers served in
    """
    # Convert string dates to date objects if necessary
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    def get_congress_number(d: date, is_start_date: bool) -> int:
        """Helper function to get Congress number for a specific date"""
        year = d.year
        # For January 3rd:
        # - If it's a start date, it belongs to the new Congress (no year adjustment)
        # - If it's an end date, it belongs to the previous Congress (adjust year)
        if d.month == 1:
            if d.day < 3 or (d.day == 3 and not is_start_date):
                year -= 1
        congress_number = ((year - 1789) // 2) + 1
        return congress_number

    # Get start and end Congress numbers
    start_congress = get_congress_number(start_date, is_start_date=True)
    end_congress = get_congress_number(end_date, is_start_date=False)

    # Generate list of all Congress numbers served
    return list(range(start_congress, end_congress + 1))


def test_congress_calculator():
    """
    Test cases for the calculate_congresses_served function.

    Raises:
        AssertionError: If any test case fails
    """
    # Test 1: Regular case spanning multiple Congresses
    assert calculate_congresses_served("2019-01-03", "2023-01-03") == [
        116,
        117,
    ], "Failed test 1: Regular case spanning multiple Congresses"

    # Test 2: Edge case - January 3rd transition day
    assert (
        calculate_congresses_served("2023-01-03", "2023-01-03") == []
    ), "Failed test 2: January 3rd transition should include 0 Congress"

    # Test 3: Start of new Congress
    assert calculate_congresses_served("2023-01-03", "2024-01-03") == [
        118
    ], "Failed test 3: Start of new Congress"

    # Test 4: Long serving politician
    assert calculate_congresses_served("2001-01-03", "2024-01-03") == list(
        range(107, 119)
    ), "Failed test 4: Long serving politician"

    # Test 5: Early January dates
    assert calculate_congresses_served("2023-01-01", "2023-01-02") == [
        117
    ], "Failed test 5: Early January dates"

    # Test 6: Start date in early January
    assert calculate_congresses_served("2023-01-02", "2023-01-04") == [
        117,
        118,
    ], "Failed test 6: Start date in early January"

    # Test 7: End date in early January
    assert calculate_congresses_served("2022-01-04", "2023-01-02") == [
        117
    ], "Failed test 7: End date in early January"

    print("All tests passed!")
