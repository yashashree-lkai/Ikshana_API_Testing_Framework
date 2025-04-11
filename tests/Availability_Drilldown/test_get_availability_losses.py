import json
from datetime import datetime
import pytest
import requests
from urllib.parse import unquote
from database_operations import fetch_data_from_database
from data.test_data import test_data
from config.config_loader import environment
from config.logging_config import logger_1
from utils.api_clint import APIClient
from conftest import normalize_timestamp, create_break_transactions


# Utility function to convert data to the required format (list of lists with values only)
def convert_to_list_of_values(api_data):
    """
    Convert API data to a list of lists, excluding the last field of each dictionary.
    :param api_data: List of dictionaries from API response
    :return: List of lists with the required values
    """
    formatted_data = []
    for item in api_data:
        record = [normalize_timestamp(value) if key in ['startTime', 'endTime'] and value else value
                  for key, value in item.items()][:-1]
        # Map 'isAttributed' field: 'False' -> 0, 'True' -> 1
        if 'isAttributed' in item:
            record[-1] = 1 if item['isAttributed'] == 'True' else 0
        formatted_data.append(record)
    return formatted_data


@pytest.mark.Downtime_Drill_down
def test_availability_losses_with_database(common_payload_attributes, get_unix_timestamps):
    """
    Test to validate downtime data between API and database.
    """
    # Initialize API Client
    api_client = APIClient()
    # Load environment variables
    base_url = environment['base_url']

    logger_1.info(
        f"Running tests against environment: {base_url}, Project Name: {test_data.PROJECT_NAME}, "
        f"Config ID: {environment['config_id']}, NODE_ID: {test_data.NODE_IDs}, Start_date:{test_data.START_TIME}, End_time:{test_data.END_TIME}"
    )
    start_timestamp, end_timestamp = get_unix_timestamps

    endpoint = '/externalUrlNew/get_availability_losses'
    request_payload = {
        "config_id": environment['config_id'],
        "node_ids": json.dumps([test_data.NODE_IDs]),
        "start_time": common_payload_attributes['start_time'],
        "end_time": common_payload_attributes['end_time'],
        "selected_filters": json.dumps({
            "part variants": [],
            "shifts": []
        })
    }
    constructed_url = f"{base_url}{endpoint}"
    prepared_request = requests.Request('GET', constructed_url, params=request_payload).prepare()
    final_url = unquote(prepared_request.url)
    logger_1.info(f"Constructed API URL: {final_url}")

    response = api_client.session.get(
        constructed_url,
        params=request_payload,
        headers={"Authorization": f"Bearer {api_client.token}"}
    )
    assert response.status_code == 200, f"API returned unexpected status: {response.status_code}"
    assert response.content, "API returned no data, skipping test."

    api_json_response = response.json()
    assert api_json_response.get("data")
    api_formatted_data = convert_to_list_of_values(api_json_response["data"])
    logger_1.info(f"API Response Data (Formatted): {len(api_formatted_data)},{api_formatted_data}")

    query = (f"""SELECT L.lossId, L.lossType, L.startTime, L.endTime, L.isAttributed, 
        CASE 
        WHEN L.endTime < {end_timestamp} THEN L.endTime ELSE {end_timestamp} 
        END - 
        CASE 
        WHEN L.startTime > {start_timestamp} THEN L.startTime ELSE {start_timestamp} 
        END AS timeLost, L.tag, L.causeOfLoss FROM Losses L 
        WHERE L.startTime between {start_timestamp} and {end_timestamp}
        AND l.isNegligible = 0 AND L.tag = 1 AND L.assetId = {test_data.NODE_IDs} 
        """)
    db_results = fetch_data_from_database(query)
    logger_1.info(f"The Data received from Database before modification:{db_results}")
    assert db_results, "Database returned no data, skipping test."

    # Process database results
    db_expected_results = []
    for db_result in db_results:
        db_start_time = normalize_timestamp(
            datetime.fromtimestamp(db_result['startTime'] / 1000).strftime("%d-%b-%Y %H:%M:%S.%f")[:-3])
        db_end_time = normalize_timestamp(
            datetime.fromtimestamp(db_result['endTime'] / 1000).strftime("%d-%b-%Y %H:%M:%S.%f")[:-3])
        db_expected_results.append([
            db_result['lossId'],
            db_result['lossType'],
            db_start_time,
            db_end_time,
            db_result['isAttributed'],
            db_result['timeLost'],
            db_result['tag'],
            db_result['causeOfLoss']
        ])

    # Exclude overlapping breaks
    start_datetime = datetime.fromtimestamp(start_timestamp / 1000)
    days_count = (end_timestamp - start_timestamp) // (24 * 60 * 60 * 1000)
    break_transactions = create_break_transactions(start_datetime, days_count)

    # Filter DB data to exclude breaks
    filtered_db_results = recalculate_time_lost(db_expected_results, break_transactions)
    logger_1.info(f"Database Data (Filtered After Break Exclusion): {len(filtered_db_results)}, "
                  f"{filtered_db_results}")

    # Compare API and database data and fetch mismatched records
    mismatched_data = compare_and_find_mismatches(api_formatted_data, filtered_db_results)
    if mismatched_data:
        logger_1.warning(f"Mismatched Data Found: {len(mismatched_data)}, {mismatched_data}")
        logger_1.warning("Test failed due to mismatched data.")
    else:
        logger_1.info("No mismatched data found. API and DB data match perfectly.")

    # Ensure no mismatches
    assert not mismatched_data, "Mismatch detected between API and DB data."
    logger_1.info("Availability losses data test passed successfully.")


def compare_and_find_mismatches(api_data, db_data):
    """
    Compares API data with database data and finds exact mismatches.
    :param api_data: List of API records
    :param db_data: List of database records
    :return: List of mismatched records with details
    """
    mismatches = []
    for index, (api_record, db_record) in enumerate(zip(api_data, db_data)):
        mismatch_details = {}
        for i, field in enumerate(['lossId', 'lossType', 'start_time', 'end_time', 'isAttributed', 'timeLost', 'tag', 'causeOfLoss']):
            if api_record[i] != db_record[i]:
                mismatch_details[field] = {
                    "api_value": api_record[i],
                    "db_value": db_record[i]
                }
        if mismatch_details:
            mismatches.append({"index": index, "mismatched_fields": mismatch_details})
    return mismatches


# Re-Calculate the time lost after excluding breaks.
def recalculate_time_lost(db_data, break_transactions):
    """
    Adjusts database records to exclude overlapping break durations, modifies time lost,
    and retains the original record structure without creating new entries.

    :param db_data: List of database records (lists)
    :param break_transactions: List of break transactions (start and end times in string format)
    :return: Adjusted database data with recalculated time lost
    """
    adjusted_data = []

    for record in db_data:
        # Extract all fields
        loss_id = record[0]
        loss_type = record[1]
        original_start = datetime.strptime(record[2], '%d-%b-%Y %H:%M:%S:%f')
        original_end = datetime.strptime(record[3], '%d-%b-%Y %H:%M:%S:%f')
        is_attributed = record[4]
        time_lost = record[5]
        tag = record[6]
        cause_of_loss = record[7]

        total_break_duration = 0

        for break_start, break_end in break_transactions:
            break_start = datetime.strptime(break_start, '%d-%b-%Y %H:%M:%S:%f')
            break_end = datetime.strptime(break_end, '%d-%b-%Y %H:%M:%S:%f')

            # If the record overlaps with a break duration
            if original_start < break_end and original_end > break_start:
                # Calculate overlap duration
                overlap_start = max(original_start, break_start)
                overlap_end = min(original_end, break_end)
                overlap_duration = (overlap_end - overlap_start).total_seconds() * 1000  # in milliseconds

                # Accumulate total break duration for subtraction
                total_break_duration += overlap_duration

                # Log the overlap information for debugging
                logger_1.debug(
                    f"Record {loss_id} ({original_start} to {original_end}) overlaps with break ({break_start} to {break_end}). "
                    f"Overlap duration: {overlap_duration} ms."
                )

                # Adjust the record's start and end times if necessary
                if original_start >= break_start and original_end <= break_end:
                    # Record is fully within the break
                    original_start = original_end  # Collapse the record
                elif original_start < break_start and original_end > break_end:
                    # Record spans the break; adjust both ends
                    original_start = original_start
                    original_end = original_end
                elif original_start < break_end <= original_end:
                    # Adjust the start time
                    original_start = break_end
                elif original_start <= break_start < original_end:
                    # Adjust the end time
                    original_end = break_start

        # Update timeLost by subtracting total break duration
        adjusted_time_lost = max(0, int(time_lost) - total_break_duration)

        # Ensure the record is still valid and retain all fields
        if original_start < original_end:
            adjusted_data.append([
                loss_id,
                loss_type,
                original_start.strftime('%d-%b-%Y %H:%M:%S:%f')[:-3],
                original_end.strftime('%d-%b-%Y %H:%M:%S:%f')[:-3],
                is_attributed,
                adjusted_time_lost,
                tag,
                cause_of_loss
            ])

    return adjusted_data


