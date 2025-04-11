import json
import requests
from urllib.parse import unquote
from data.test_data import test_data
from config.environment import environment
from config.logging_config import logger_1
from utils.api_clint import APIClient


def test_get_downtime_data_with_database(common_payload_attributes, get_unix_timestamps):
    """
    Test to validate downtime data between API and database for 'get_downtimes_data'.
    """
    api_client = APIClient()
    base_url = environment['base_url']
    node_config_ids = [f"{test_data.NODE_IDs}, {environment['config_id']}"]

    logger_1.info(
        f"Running tests against environment: {base_url}, Project Name: {test_data.PROJECT_NAME}, "
        f"Config ID: {environment['project_id']}, NODE_ID: {test_data.NODE_IDs}, "
        f"Start_date:{test_data.START_TIME}, End_time:{test_data.END_TIME}"
    )

    start_timestamp, end_timestamp = get_unix_timestamps

    endpoint = '/externalUrlNew/get_state_graph_data'
    request_payload = {
        "project_id": environment['project_id'],
        "start_time": common_payload_attributes['start_time'],
        "end_time": common_payload_attributes['end_time'],
        "node_config_ids": json.dumps(node_config_ids),
        "include_subassembly": "false"
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
    for node_config_id in node_config_ids:
        API_data = api_json_response["data"].get(node_config_id, [])
        logger_1.info(f"Data fetched from API: {len(API_data)},{API_data}")
