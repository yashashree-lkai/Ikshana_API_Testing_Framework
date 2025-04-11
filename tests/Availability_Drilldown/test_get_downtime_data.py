import json
from datetime import datetime

import pytest
import requests
from urllib.parse import unquote
from database_operations import fetch_data_from_database
from data.test_data import test_data
from config.environment import environment
from config.logging_config import logger_1
from utils.api_clint import APIClient
from conftest import normalize_timestamp, create_break_transactions


def convert_instances_to_values(api_instances):
    """
    Convert API instance data to a list of lists with required fields only.
    :param api_instances: List of dictionaries representing instances from API response
    :return: List of lists containing the instance values
    """
    formatted_data = []
    for instance in api_instances:
        formatted_record = [
            normalize_timestamp(instance.get('start_time')),
            normalize_timestamp(instance.get('end_time')),
            instance.get('duration'),  # Convert duration to int for consistency
            instance.get('node_id', None),
        ]
        formatted_data.append(formatted_record)
    return formatted_data


@pytest.mark.Downtime_Drill_down
def test_get_downtime_data_with_database(common_payload_attributes, get_unix_timestamps):
    """
    Test to validate downtime data between API and database for 'get_downtimes_data'.
    """
    api_client = APIClient()
    base_url = environment['base_url']

    logger_1.info(
        f"Running tests against environment: {base_url}, Project Name: {test_data.PROJECT_NAME}, "
        f"Config ID: {environment['config_id']}, NODE_ID: {test_data.NODE_IDs}, Start_date:{test_data.START_TIME}, End_time:{test_data.END_TIME}"
    )

    start_timestamp, end_timestamp = get_unix_timestamps

    endpoint = '/externalUrlNew/get_downtimes_data'
    request_payload = {
        "config_id": environment['config_id'],
        "node_ids": f"[{test_data.NODE_IDs}]",
        "start_time": common_payload_attributes['start_time'],
        "end_time": common_payload_attributes['end_time']
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
    assert response.status_code == 200, f"API returned unexpected status: {response.status_code}, Response: {response.text}"
    assert response.content, "API returned no data, skipping test."

    api_json_response = response.json()
    assert api_json_response.get("data"), "API returned empty or invalid data."

    show_by_duration = api_json_response["data"].get("show_by_duration", [])
    api_instances_data_duration = []
    for duration_record in show_by_duration:
        api_instances_data_duration.extend(convert_instances_to_values(duration_record["instances"]))

    show_by_count = api_json_response["data"].get("show_by_count", [])
    api_instances_data_count = []
    for count_record in show_by_count:
        api_instances_data_count.extend(convert_instances_to_values(count_record["instances"]))

    logger_1.info(
        f"API 'show_by_duration' Instances Data: {len(api_instances_data_duration)},{api_instances_data_duration}")
    logger_1.info(f"API 'show_by_count' Instances Data: {len(api_instances_data_count)}, {api_instances_data_count}")

    query_duration = (
        f"SELECT starttime, endtime, ROUND((endtime - starttime), 2) AS duration, nodeid "
        f"FROM ProductionIntervals WHERE starttime<{end_timestamp} and endtime>{start_timestamp} "
        f"AND is_production = 0 AND nodeid = {test_data.NODE_IDs} order by starttime"
    )
    db_results_duration = fetch_data_from_database(query_duration)
    assert db_results_duration, "Database returned no data, skipping test."

    db_expected_results_duration = [
        [
            normalize_timestamp(
                datetime.fromtimestamp(record['starttime'] / 1000).strftime("%d-%b-%Y %H:%M:%S.%f")[:-3]),
            normalize_timestamp(datetime.fromtimestamp(record['endtime'] / 1000).strftime("%d-%b-%Y %H:%M:%S.%f")[:-3]),
            record['duration'],
            record['nodeid'],
        ]
        for record in db_results_duration
    ]

    # Adjusting the start_datetime based on whether it's exactly midnight or near midnight
    start_datetime = datetime.fromtimestamp(start_timestamp / 1000)
    end_datetime = datetime.fromtimestamp(end_timestamp / 1000)
    days_count = (end_timestamp - start_timestamp) // (24 * 60 * 60 * 1000)
    break_transactions = create_break_transactions(start_datetime, end_datetime, days_count)
    filtered_duration_db_data = filter_data_excluding_breaks(db_expected_results_duration, break_transactions)

    logger_1.info(f"Database Data (Filtered) for duration: {len(filtered_duration_db_data)}{filtered_duration_db_data}")

    mismatches_duration = compare_and_find_mismatches(api_instances_data_duration, filtered_duration_db_data)
    assert not mismatches_duration, f"Mismatch in 'show_by_duration' data: {json.dumps(mismatches_duration, indent=2)}"

    query_count = (
        f"SELECT starttime, endtime, ROUND((endtime - starttime), 2) AS duration "
        f"FROM ProductionIntervals WHERE starttime<{end_timestamp} and endtime>{start_timestamp} "
        f"AND is_production = 0 AND nodeid = {test_data.NODE_IDs} order by starttime;"
    )
    db_results_count = fetch_data_from_database(query_count)
    assert db_results_count, "Database returned no data, skipping test."

    db_expected_results_count = [
        [
            normalize_timestamp(
                datetime.fromtimestamp(record['starttime'] / 1000).strftime("%d-%b-%Y %H:%M:%S.%f")[:-3]),
            normalize_timestamp(datetime.fromtimestamp(record['endtime'] / 1000).strftime("%d-%b-%Y %H:%M:%S.%f")[:-3]),
            record['duration'],
            None,
        ]
        for record in db_results_count
    ]
    filtered_count_db_data = filter_data_excluding_breaks(db_expected_results_count, break_transactions)
    logger_1.info(f"Database Data (Filtered) for count:{len(filtered_count_db_data)} {filtered_count_db_data}")

    mismatches_count = compare_and_find_mismatches(api_instances_data_count, filtered_count_db_data)
    assert not mismatches_count, f"Mismatch in 'show_by_count' data: {json.dumps(mismatches_count, indent=2)}"


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
        for i, field in enumerate(['start_time', 'end_time', 'duration', 'node_id']):
            if api_record[i] != db_record[i]:
                mismatch_details[field] = {
                    "api_value": api_record[i],
                    "db_value": db_record[i]
                }
        if mismatch_details:
            mismatches.append({"index": index, "mismatched_fields": mismatch_details})
    return mismatches


def filter_data_excluding_breaks(db_data, break_transactions):
    """
    Adjusts transaction durations by excluding overlapping break durations within the same day.
    :param db_data: List of database records (lists)
    :param break_transactions: List of break transactions (start and end times in string format)
    :return: Adjusted database data with recalculated duration values
    """

    def deduplicate_and_sort_breaks(breaks):
        """
        Deduplicates and sorts the break transactions by their start time.
        :param breaks: List of break transactions (start and end times in string format)
        :return: Sorted and deduplicated break transactions
        """
        unique_breaks = list(set(tuple(b) for b in breaks))
        sorted_breaks = sorted(unique_breaks, key=lambda x: datetime.strptime(x[0], '%d-%b-%Y %H:%M:%S:%f'))
        return sorted_breaks

    break_transactions = deduplicate_and_sort_breaks(break_transactions)

    adjusted_data = []

    for record in db_data:
        transaction_start = datetime.strptime(record[0], '%d-%b-%Y %H:%M:%S:%f')
        transaction_end = datetime.strptime(record[1], '%d-%b-%Y %H:%M:%S:%f')
        node_id = record[3]

        # Define the selected time range
        selected_start_time = datetime.strptime(test_data.START_TIME, '%d-%b-%Y %H:%M:%S:%f')
        selected_end_time = datetime.strptime(test_data.END_TIME, '%d-%b-%Y %H:%M:%S:%f')

        # Case 1: Transaction starts and ends within the selected time range (S1 to S2)
        if transaction_start > selected_start_time and transaction_end < selected_end_time:
            adjusted_duration = (transaction_end - transaction_start).total_seconds() * 1000

        # Case 2: Transaction starts before selected start time (S1) and ends within the selected range (S1 to S2)
        elif transaction_start < selected_start_time and transaction_end < selected_end_time:
            adjusted_duration = (transaction_end - selected_start_time).total_seconds() * 1000

        # Case 3: Transaction starts within the selected range (S1 to S2) and ends after the selected end time (S2)
        elif transaction_start > selected_start_time and transaction_end > selected_end_time:
            adjusted_duration = (selected_end_time - transaction_start).total_seconds() * 1000

        else:
            # No overlap with the selected time range, set duration to 0
            adjusted_duration = 0

        # Subtract break durations overlapping with the transaction period
        total_break_time = 0
        for break_start, break_end in break_transactions:
            break_start = datetime.strptime(break_start, '%d-%b-%Y %H:%M:%S:%f')
            break_end = datetime.strptime(break_end, '%d-%b-%Y %H:%M:%S:%f')

            # Check for overlap between the transaction and the break
            if break_start <= transaction_end and break_end >= transaction_start:
                overlap_start = max(transaction_start, break_start)
                overlap_end = min(transaction_end, break_end)
                overlapping_time = int((overlap_end - overlap_start).total_seconds() * 1000)

                total_break_time += overlapping_time

        # Adjust the transaction duration after subtracting break times
        adjusted_duration = max(0, adjusted_duration - total_break_time)

        # Append the adjusted record
        adjusted_data.append([record[0], record[1], adjusted_duration, node_id])

    return adjusted_data




