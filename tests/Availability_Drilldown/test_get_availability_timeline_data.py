import json
from datetime import datetime
from urllib.parse import unquote
import pytest
import requests
from database_operations import fetch_data_from_database
from config.environment import environment
from config.logging_config import logger_1
from data.test_data import test_data
from utils.api_clint import APIClient
from conftest import compare_api_db_data

# State type mapping
STATE_MAPPING = {
    0: "CYCLING",
    1: "BLOCKED_STARVED",
    2: "INTERFACING",
    3: "DOWN",
    4: "UNKNOWN",
    5: "BLOCKED",
    6: "STARVED",
    7: "LOADING",
    8: "UNLOADING"
}


@pytest.mark.Downtime_Drill_down
def test_get_availability_timeline_data(common_payload_attributes, get_unix_timestamps):
    # Initialize API Client, load environment, etc.
    api_client = APIClient()
    base_url = environment['base_url']

    logger_1.info(
        f"Running tests against environment: {base_url}, Project Name: {test_data.PROJECT_NAME}, "
        f"Config ID: {environment['config_id']}, NODE_ID: {test_data.NODE_IDs}, "
        f"Start_date:{ test_data.START_TIME}, End_time:{ test_data.END_TIME}")

    start_timestamp, end_timestamp = get_unix_timestamps

    # Prepare and send the API request
    endpoint = '/externalUrlNew/get_availability_timeline_data'
    request_payload = {
        "config_id": environment['config_id'],
        "start_time": common_payload_attributes['start_time'],
        "end_time": common_payload_attributes['end_time'],
        "node_ids": json.dumps([test_data.NODE_IDs])
    }
    constructed_url = f"{base_url}{endpoint}"
    prepared_request = requests.Request('GET', constructed_url, params=request_payload).prepare()
    final_url = unquote(prepared_request.url)
    logger_1.info(f"Constructed API URL: {final_url}")

    # Send API request and get response
    response = api_client.session.get(
        constructed_url,
        params=request_payload,
        headers={"Authorization": f"Bearer {api_client.token}"}
    )
    assert response.status_code == 200, f"API returned unexpected status: {response.status_code}"
    api_json_response = response.json()
    api_data = api_json_response.get("data")
    logger_1.info(f"The data returned from API:{len(api_data)}, {api_data}")

    # Fetch data from the database
    query = (
        f"""SELECT nodeid,
         FORMAT(DATEADD(SECOND, CAST(startTime AS BIGINT) / 1000 + 19800, '1970-01-01 00:00:00'), 'dd MMM yyyy HH:mm:ss') 
         + ':' + RIGHT('000' + CAST(CAST(startTime AS BIGINT) % 1000 AS VARCHAR), 3) AS startTime,
         FORMAT(DATEADD(SECOND, CAST(endTime AS BIGINT) / 1000 + 19800, '1970-01-01 00:00:00'), 'dd MMM yyyy HH:mm:ss') 
         + ':' + RIGHT('000' + CAST(CAST(endTime AS BIGINT) % 1000 AS VARCHAR), 3) AS endTime,
         state_type FROM AbstractStateTransactions WHERE nodeid = {test_data.NODE_IDs}
         AND startTime < {end_timestamp}
         AND endTime > {start_timestamp};"""
    )
    db_results = fetch_data_from_database(query)
    assert db_results, logger_1.warning(f"Database returned no data for following query: {query}")
    logger_1.info(f"The database data without filtered:{len(db_results)}, {db_results}")

    # Transform DB output to match API format
    filtered_db_data = transform_db_output(db_results)
    logger_1.info(f"The Filtered database data is:{filtered_db_data}")

    # Now compare API data and transformed DB data
    mismatch_data = compare_api_db_data(api_data, filtered_db_data)

    assert not mismatch_data, f"Data mismatch found: {len(mismatch_data)} mismatches. {mismatch_data}"


def transform_db_output(db_records):
    """
    Transform DB records to match the API response structure.

    Parameters:
        db_records (list of dict): Each dict represents a row in the DB output.

    Returns:
        list of dict: Transformed records matching the API response format.
    """
    transformed_records = []

    for record in db_records:
        # Parse the start and end times directly as they are already strings
        start_time = datetime.strptime(record["startTime"], "%d %b %Y %H:%M:%S:%f")
        end_time = datetime.strptime(record["endTime"], "%d %b %Y %H:%M:%S:%f")
        state_type = int(record["state_type"])
        state_label = STATE_MAPPING[state_type]

        # Add a single entry for start time (or adjust as needed)
        transformed_records.append({"timestamp": start_time, state_label: 100})
        transformed_records.append({"timestamp": end_time, state_label: 0})

    # Sort the transformed records by the timestamp
    transformed_records.sort(key=lambda x: x["timestamp"])

    # Convert timestamps back to formatted strings
    for record in transformed_records:
        record["timestamp"] = record["timestamp"].strftime("%d-%b-%Y %H:%M:%S:%f")[:-3]
    return transformed_records


def exclude_timestamps_overlapping_breaks(data, break_transactions):
    """
    Exclude timestamps overlapping with break transactions from the data.

    Parameters:
        data (list of dict): Data containing 'timestamp' and state information.
        break_transactions (list of list): List of break transactions.

    Returns:
        list of dict: Filtered data.
    """
    filtered_data = []

    for record in data:
        timestamp = datetime.strptime(record["timestamp"], "%d-%b-%Y %H:%M:%S:%f")

        # Check if the timestamp overlaps with any break
        in_break = False
        for break_start, break_end in break_transactions:
            break_start = datetime.strptime(break_start, '%d-%b-%Y %H:%M:%S:%f')
            break_end = datetime.strptime(break_end, '%d-%b-%Y %H:%M:%S:%f')
            if break_start <= timestamp <= break_end:
                in_break = True
                break

        # Add to filtered data if not in a break
        if not in_break:
            filtered_data.append(record)

    return filtered_data
