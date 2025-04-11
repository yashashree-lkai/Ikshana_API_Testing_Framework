import json

import pytest

from database_operations import fetch_data_from_database
from config.environment import environment
from data.test_data import test_data
from utils.api_clint import APIClient
from config.logging_config import logger_1


@pytest.mark.Loss_Analysis
def test_count_response_with_database(common_payload_attributes, get_unix_timestamps):
    # Initialize API Client
    api_client = APIClient()
    # Load environment variables
    base_url = environment['base_url']

    start_timestamp, end_timestamp = get_unix_timestamps

    logger_1.info(
        f"Running tests against environment: {base_url}, Project Name: {test_data.PROJECT_NAME}, "
        f"Project ID: {environment['project_id']}, NODE_ID: {test_data.NODE_IDs}, "
        f"Start_date:{test_data.START_TIME}, End_time:{test_data.END_TIME}"
    )

    node_query = """
               (SELECT DISTINCT n 
                FROM TNode 
                WHERE btField = 3 AND iStatus = 1 AND sInfo IS NULL)
               INTERSECT 
               (SELECT DISTINCT assetId FROM Losses)
           """

    # Fetch node data from the database
    assets_data = fetch_data_from_database(node_query)
    node_ids = [node['n'] for node in assets_data]  # Extract node IDs from the fetched data
    node_ids_json = json.dumps(node_ids)

    endpoint = '/externalUrlNew/asset_wise_loss_type'
    payload = {
        "project_id": environment['project_id'],
        "start_time": test_data.START_TIME,
        "end_time": test_data.END_TIME,
        "selected_filters": json.dumps({"shifts": [], "part variants": []}),
        "node_ids": node_ids_json
    }
    constructed_url = f"{base_url}{endpoint}"
    logger_1.info(f"Constructed API URL: {constructed_url}")

    response = api_client.session.post(
        constructed_url,
        json=payload,
        headers={"Authorization": f"Bearer {api_client.token}", "Content-Type": "application/json"}
    )
    logger_1.info(f"response Content:{response.content}")
    assert response.status_code == 200, f"API returned unexpected status: {response.status_code}"
    assert response.content, "API returned no data, skipping test."

    api_json_response = response.json()
    logger_1.info(f"API Json Response:{api_json_response}")
    assert api_json_response.get("data"), "API returned empty or invalid data."

    parsed_api_response_count = []

    # Ensure we are iterating over the correct list
    for item in api_json_response.get("data", {}).get("count", []):
        if not isinstance(item, dict):
            logger_1.error(f"Unexpected response format: {item}")
            continue
        asset_ids = item.get('asset_id', '').split(',')
        asset_id = asset_ids[0].strip()
        for loss_type, count in item.items():
            if loss_type != 'asset_id' and loss_type.isdigit():
                parsed_api_response_count.append({
                    'asset_id': asset_id.strip(),
                    'loss_type': loss_type,
                    'count': count
                })

    logger_1.info(f"Parsed API Response for count:{len(parsed_api_response_count)}, {parsed_api_response_count}")

    parsed_api_response_duration = []

    # Ensure we are iterating over the correct list
    for item in api_json_response.get("data", {}).get("duration", []):
        if not isinstance(item, dict):
            logger_1.error(f"Unexpected response format: {item}")
            continue
        asset_ids = item.get('asset_id', '').split(',')
        asset_id = asset_ids[0].strip()
        for loss_type, duration in item.items():
            if loss_type != 'asset_id' and loss_type.isdigit():
                parsed_api_response_duration.append({
                    'asset_id': asset_id.strip(),
                    'loss_type': loss_type,
                    'duration': duration
                })

    logger_1.info(f"Parsed API Response for duration:{len(parsed_api_response_duration)}, {parsed_api_response_duration}")

    count_query = (
        f"""(SELECT assetId, lossType, COUNT(*) as Count FROM Losses, LossTypes, TNode WHERE Losses.assetId = TNode.n
        AND Losses.lossType = LossTypes.lossTypeId AND isAttributed = 1 AND startTime >= {start_timestamp}
        AND endTime <= {end_timestamp} AND Losses.timeLost > 0 GROUP BY assetId, sName, lossType, lossTypeName)
        UNION (SELECT assetId, ISNULL(lossType, '32'), COUNT(*) as Count FROM Losses, LossTypes, TNode
        WHERE Losses.assetId = TNode.n AND isAttributed = 0 AND startTime >= {start_timestamp}
        AND endTime <= {end_timestamp} AND Losses.timeLost > 0 GROUP BY assetId, lossType) ORDER BY assetId;""")

    database_data = fetch_data_from_database(count_query)
    assert database_data, "Database query returned None, skipping test."

    organized_database_data_count = organized_data = []
    for row in database_data:
        organized_data.append({
            'asset_id': row['assetId'],
            'loss_type': row['lossType'],
            'count': row['Count']
        })

    logger_1.info(f"Organized Database Data for count: {len(organized_database_data_count)} {organized_database_data_count}")

    duration_query = (
        f"""(select assetId, lossType, sum(cast(timeLost as numeric (18,2)))/60000 duration 
                from Losses L, LossTypes LT, TNode TN where L.assetId = TN.n and L.lossType = LT.lossTypeId 
                and isAttributed = 1 and startTime >= {start_timestamp} and endTime <= {end_timestamp}
                and L.timeLost > 0 group by assetId, sName, lossType, lossTypeName) 
                union 
                (select assetId, ISNULL(lossType, '32'), sum(cast(timeLost as numeric (18,2)))/60000 as duration 
                from Losses L, LossTypes LT, TNode TN where L.assetId = TN.n and isAttributed = 0 
                and startTime >= {start_timestamp} and endTime <= {end_timestamp} 
                and L.timeLost > 0 group by assetId, lossType) 
                order by assetId""")

    database_data = fetch_data_from_database(duration_query)
    assert database_data, "Database query returned None, skipping test."

    organized_database_data_duration = organized_data = []
    for row in database_data:
        organized_data.append({
            'asset_id': row['assetId'],
            'loss_type': row['lossType'],
            'duration': row['duration']
        })

    logger_1.info(
        f"Organized Database Data for duration: {len(organized_database_data_duration)} {organized_database_data_duration}")

    errors = []

    try:
        assert compare_records(parsed_api_response_count, organized_database_data_count, 'count'), ("Count comparison "
                                                                                                    "failed.")
        logger_1.info("Count comparison successful.")
    except AssertionError as e:
        logger_1.error(f"Count comparison failed: {e}")
        errors.append(f"Count comparison failed: {e}")

    try:
        assert compare_records(parsed_api_response_duration,
                               organized_database_data_duration, 'duration'), "Duration comparison failed."
        logger_1.info("Duration comparison successful.")
    except AssertionError as e:
        logger_1.error(f"Duration comparison failed: {e}")
        errors.append(f"Duration comparison failed: {e}")

    for error in errors:
        pytest.fail(error)


def compare_records(api_response, db_data, key_fields):
    """
    Compares API response records with database records based on specified key fields.

    :param api_response: List of dictionaries representing API response data
    :param db_data: List of dictionaries representing database data
    :param key_fields: List of field names to compare (e.g., ['count'])

    :return: List of mismatched records
    """
    mismatches = []

    # Convert db_data to a lookup dictionary for faster access
    db_lookup = {(str(item['asset_id']), str(item['loss_type'])): item for item in db_data}

    for api_item in api_response:
        asset_id, loss_type = str(api_item['asset_id']), str(api_item['loss_type'])
        db_item = db_lookup.get((asset_id, loss_type))

        if not db_item:
            mismatches.append({
                'asset_id': asset_id,
                'loss_type': loss_type,
                'status': 'Missing in DB'
            })
            continue

        # Compare specified fields
        for field in key_fields:
            api_value = api_item.get(field)
            db_value = db_item.get(field)
            if api_value != db_value:
                mismatches.append({
                    'asset_id': asset_id,
                    'loss_type': loss_type,
                    'field': field,
                    'api_value': api_value,
                    'db_value': db_value,
                    'status': 'Mismatch'
                })

    assert (len(api_response)) == len(db_data)
    return mismatches
