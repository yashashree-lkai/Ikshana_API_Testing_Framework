import json
import os
from datetime import datetime, timedelta
import pytest
import pytz

import config.config_loader
from config.logging_config import setup_logging, logger_1
from data.test_data import test_data
from database_operations import get_breaks_configurations, get_shift_configurations
from utils.api_client import APIClient


# Fixture to fetch and validate start and end timestamps
@pytest.fixture
def get_unix_timestamps():
    def correct_time_format(time_str):
        try:
            return datetime.strptime(time_str, "%d-%b-%Y %H:%M:%S:%f")
        except ValueError as e:
            raise ValueError(f"Invalid time format: {time_str}. Error: {e}")

    # Extract configuration from test_data
    start_time = test_data.START_TIME
    end_time = test_data.END_TIME
    selected_timezone = test_data.LOCAL_TIME

    if not start_time or not end_time:
        raise ValueError("Environment variables 'START_TIME' and 'END_TIME' must be set.")

    if not selected_timezone:
        raise ValueError("Environment variable 'LOCAL_TIME' must be set to a valid timezone.")

    try:
        # Initialize the selected timezone
        tz = pytz.timezone(selected_timezone)
    except pytz.UnknownTimeZoneError:
        raise ValueError(f"Invalid timezone: {selected_timezone}")

    # Parse and localize the start and end times
    start_datetime = tz.localize(correct_time_format(start_time))
    end_datetime = tz.localize(correct_time_format(end_time))

    # Calculate Unix timestamps directly in the selected timezone
    start_timestamp = int(start_datetime.timestamp() * 1000)
    end_timestamp = int(end_datetime.timestamp() * 1000)

    # Log the output
    logger_1.info(f"Selected Timezone: {selected_timezone}")
    logger_1.info(f"Start Timestamp: {start_timestamp} ({start_datetime})")
    logger_1.info(f"End Timestamp: {end_timestamp} ({end_datetime})")

    return start_timestamp, end_timestamp


def normalize_timestamp(timestamp_str):
    """
    Normalize a timestamp string to a uniform format.
    Example: Convert datetime string to "%d-%b-%Y %H:%M:%S.%f" format.
    """
    try:
        if timestamp_str.count(":") > 2:
            parts = timestamp_str.rsplit(":", 1)
            timestamp_str = ".".join(parts)

        timestamp = datetime.strptime(timestamp_str, "%d-%b-%Y %H:%M:%S.%f")
        normalized_timestamp = timestamp.strftime("%d-%b-%Y %H:%M:%S.%f")[:-3]

        if "." in normalized_timestamp:
            return normalized_timestamp.rsplit(".", 1)[0] + ":" + normalized_timestamp.rsplit(".", 1)[1]

        return normalized_timestamp
    except ValueError as e:
        raise ValueError(f"Invalid timestamp format: {timestamp_str}. Error: {e}")


@pytest.fixture
def common_payload_attributes(get_unix_timestamps):
    start_timestamp, end_timestamp = get_unix_timestamps

    start_time = normalize_timestamp(
        datetime.fromtimestamp(start_timestamp / 1000).strftime("%d-%b-%Y %H:%M:%S.%f")[:-3]
    )
    end_time = normalize_timestamp(
        datetime.fromtimestamp(end_timestamp / 1000).strftime("%d-%b-%Y %H:%M:%S.%f")[:-3]
    )

    return {'start_time': start_time, 'end_time': end_time}


@pytest.fixture(scope="module")
def api_client():
    client = APIClient()
    yield client
    client.logout()


def pytest_configure():
    setup_logging()
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    reports_dir = os.path.join(os.path.dirname(__file__), 'Reports')
    os.makedirs(reports_dir, exist_ok=True)
    html_report_file = os.path.join(reports_dir, f'test_report_{timestamp}.html')
    config.config_loader.REPORTS_DIR = html_report_file


def pytest_html_report_title(report):
    report.title = "Custom Test Report with Environment Details"


# def pytest_html_results_summary(prefix, summary, postfix):
#     prefix.extend([
#         f"<h3>Environment Details</h3>",
#         f"<p><strong>Base URL:</strong> {CONFIG['base_url']}</p>",
#         f"<p><strong>Config ID:</strong> {CONFIG['config_id']}</p>",
#     ])


def pytest_addoption(parser):
    parser.addoption('--env', action='store', default='qa')


# Utility to compare API and DB data
def compare_api_db_data(api_data, db_data, return_mismatched=False):
    """
    Compare API data with DB data and return mismatches if required, ensuring only one mismatch entry for each record.

    Parameters:
        api_data (list of dict or list of list): API response data.
        db_data (list of dict or list of list): Transformed database data.
        return_mismatched (bool): If True, return mismatched records.

    Returns:
        bool or list: True if data matches; mismatched records if return_mismatched is True.
    """
    mismatches = []

    # Track records already processed to avoid duplicates
    processed_api_records = set()
    processed_db_records = set()

    # Convert dictionary records to a hashable type (e.g., JSON string)
    def convert_to_hashable(record):
        # If the record is a dictionary, convert it to a JSON string
        if isinstance(record, dict):
            return json.dumps(record, sort_keys=True)
        # If it's a list of dictionaries, convert each dictionary to a JSON string and make a tuple
        elif isinstance(record, list):
            return tuple(json.dumps(r, sort_keys=True) for r in record)
        return record

    # Find missing in DB
    for api_record in api_data:
        hashable_api_record = convert_to_hashable(api_record)
        if hashable_api_record not in processed_db_records:
            mismatches.append(api_record)
            processed_api_records.add(hashable_api_record)  # Mark as processed

    # Find missing in API
    for db_record in db_data:
        hashable_db_record = convert_to_hashable(db_record)
        if hashable_db_record not in processed_api_records:
            mismatches.append(db_record)
            processed_db_records.add(hashable_db_record)

    # Log mismatches only if there are any
    if mismatches:
        logger_1.warning(f"Mismatched Data Found: {len(mismatches)} entries. Mismatched data: {mismatches}")
    else:
        logger_1.info("No mismatches found between API and DB data.")

    if return_mismatched:
        return mismatches
    return not mismatches


def create_break_transactions(start_datetime, end_datetime, days_count):
    """
    Generate break transactions based on shifts and breaks configurations within the specified date range.
    :param start_datetime: The starting datetime for the break transactions
    :param end_datetime: The ending datetime for the break transactions
    :param days_count: The number of days to generate transactions
    :return: List of break transactions
    """
    shifts = get_shift_configurations()  # [(Shift_Start_Time, Shift_End_Time)]
    breaks = get_breaks_configurations()  # [(Break_Start_Time_ms, Break_End_Time_ms)]

    # Extract the start and end date
    start_date = start_datetime.date()
    end_date = end_datetime.date()

    break_transactions = []

    # Function to add break transactions for a specific day
    def add_breaks_for_day(current_date):
        day_start_datetime = datetime.combine(current_date, datetime.min.time())
        day_end_datetime = datetime.combine(current_date, datetime.max.time())

        # Only generate breaks for this day if any breaks are within the time range
        day_breaks_found = False

        # Loop through each shift and break time
        for shift_start_time, shift_end_time in shifts:
            shift_start_datetime = datetime.combine(current_date, shift_start_time)
            shift_end_datetime = datetime.combine(current_date, shift_end_time)

            # Handle overnight shifts by extending shift end time
            if shift_end_datetime < shift_start_datetime:
                shift_end_datetime += timedelta(days=1)

            for break_start_ms, break_end_ms in breaks:
                # Calculate break start and end times relative to the day's 00:00 timestamp
                break_start_datetime = day_start_datetime + timedelta(milliseconds=break_start_ms)
                break_end_datetime = day_start_datetime + timedelta(milliseconds=break_end_ms)

                # Check if the break is within the selected start and end time range
                if break_end_datetime > start_datetime and break_start_datetime < end_datetime:
                    # Only add breaks that fall within the shift range
                    if shift_start_datetime <= break_start_datetime < shift_end_datetime:
                        # Check if the exact same break has already been added for the current day
                        new_break = [
                            break_start_datetime.strftime('%d-%b-%Y %H:%M:%S:%f')[:-3].replace('.', ':'),
                            break_end_datetime.strftime('%d-%b-%Y %H:%M:%S:%f')[:-3].replace('.', ':')
                        ]

                        # Avoid duplicates: Only append if not already in the list for the current date
                        if new_break not in break_transactions:
                            break_transactions.append(new_break)
                            day_breaks_found = True

        # Only return the break transactions for this day if there were any valid breaks
        return day_breaks_found

    # Step 1: Include breaks for one day before the start date if there are breaks in range
    prev_date = start_date - timedelta(days=1)
    add_breaks_for_day(prev_date)

    # Step 2: Generate break transactions for the requested date range
    current_date = start_date
    while current_date <= end_date:
        add_breaks_for_day(current_date)
        current_date += timedelta(days=1)

    # Step 3: Include breaks for one day after the end date if there are breaks in range
    next_date = end_date + timedelta(days=1)
    add_breaks_for_day(next_date)

    # Log the generated break transactions for debugging
    logger_1.info(f"Break Transactions: {break_transactions}")

    # Return the break transactions, but only if there are valid ones
    return break_transactions if break_transactions else []


def normalize_breaks(break_transactions):
    """
    Convert break transactions to the normalized string format.
    """
    normalized = []
    for br in break_transactions:
        start_time = br[0]  # break starttime
        end_time = br[1]  # Break endtime

        # Convert strings to datetime if necessary
        if isinstance(start_time, str):
            start_time = datetime.strptime(start_time, '%d-%b-%Y %H:%M:%S:%f')

        if isinstance(end_time, str):
            end_time = datetime.strptime(end_time, '%d-%b-%Y %H:%M:%S:%f')

        # Normalize to string format with the desired format
        normalized.append([
            start_time.strftime('%d-%b-%Y %H:%M:%S:%f')[:-3].replace('.', ':'),
            end_time.strftime('%d-%b-%Y %H:%M:%S:%f')[:-3].replace('.', ':')
        ])
    return normalized


# """This function is completely created for timelost columns only, if you want to apply this function at any place
# just replace the .....[record[]]....wherever required."""
# # Function to filter out overlapping break durations
# def filter_data_excluding_breaks(db_data, break_transactions):
#     """
#         Filters database data to exclude overlapping break durations, adjusts time lost,
#         and creates new records for non-overlapping segments.
#         :param db_data: List of database records (lists)
#         :param break_transactions: List of break transactions (start and end times in string format)
#         :return: Adjusted database data with recalculated time lost
#         """
#     adjusted_data = []
#
#     for record in db_data:
#         original_start = datetime.strptime(record[2], '%d-%b-%Y %H:%M:%S:%f')
#         original_end = datetime.strptime(record[3], '%d-%b-%Y %H:%M:%S:%f')
#         current_start = original_start
#         current_end = original_end
#
#         for break_start, break_end in break_transactions:
#             break_start = datetime.strptime(break_start, '%d-%b-%Y %H:%M:%S:%f')
#             break_end = datetime.strptime(break_end, '%d-%b-%Y %H:%M:%S:%f')
#
#             # If the record overlaps with a break duration
#             if current_start < break_end and current_end > break_start:
#                 # If the break splits the record into two segments
#                 if current_start < break_start and current_end > break_end:
#                     # Add the segment before the break
#                     adjusted_data.append(record[:2] + [
#                         current_start.strftime('%d-%b-%Y %H:%M:%S:%f'),
#                         break_start.strftime('%d-%b-%Y %H:%M:%S:%f'),
#                         record[5],
#                         int((break_start - current_start).total_seconds() * 1000)
#                     ])
#                     # Adjust current start to after the break
#                     current_start = break_end
#                 # If the record starts before and ends during the break
#                 elif current_start < break_start and current_end <= break_end:
#                     adjusted_data.append(record[:2] + [
#                         current_start.strftime('%d-%b-%Y %H:%M:%S:%f'),
#                         break_start.strftime('%d-%b-%Y %H:%M:%S:%f'),
#                         record[5],
#                         int((break_start - current_start).total_seconds() * 1000)
#                     ])
#                     current_start = break_end
#                 # If the record starts during the break
#                 elif current_start >= break_start and current_start < break_end:
#                     current_start = break_end
#
#         # If there's any remaining segment after all breaks, add it
#         if current_start < current_end:
#             adjusted_data.append(record[:2] + [
#                 current_start.strftime('%d-%b-%Y %H:%M:%S:%f'),
#                 current_end.strftime('%d-%b-%Y %H:%M:%S:%f'),
#                 record[5],
#                 int((current_end - current_start).total_seconds() * 1000)
#             ])
#
#     return adjusted_data










