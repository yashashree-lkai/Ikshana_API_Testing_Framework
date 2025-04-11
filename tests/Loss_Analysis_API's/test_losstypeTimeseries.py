import pytest

from database_operations import fetch_data_from_database
from config.environment import environment
from data.test_data import test_data
from utils.api_clint import APIClient
from config.logging_config import logger_1
from conftest import compare_api_db_data


@pytest.mark.Loss_Analysis
def test_count_response_with_database(common_payload_attributes, get_unix_timestamps):
    # Initialize API Client
    api_client = APIClient()
    # Load environment variables
    base_url = environment['base_url']

    logger_1.info(
        f"Running tests against environment: {base_url}, Project Name: {test_data.PROJECT_NAME}, "
        f"Project ID: {environment['project_id']}, NODE_ID: {test_data.NODE_IDs}, "
        f"Start_date:{test_data.START_TIME}, End_time:{test_data.END_TIME}"
    )

    endpoint = '/externalUrlNew/loss_type_timeseries'
    payload = {
        "project_id": environment['project_id'],
        "start_time": test_data.START_TIME,
        "end_time": test_data.END_TIME,
        "selected_filters": "{\"shifts\":[],\"part variants\":[]}",
        "shift_to_shift": True
    }

    constructed_url = f"{base_url}{endpoint}"
    logger_1.info(f"Constructed API URL for count data: {constructed_url}")

    response = api_client.session.post(
        constructed_url,
        json=payload,
        headers={"Authorization": f"Bearer {api_client.token}", "Content-Type": "application/json"}
    )
    assert response.status_code == 200, f"API returned unexpected status: {response.status_code}"
    assert response.content, "API returned no data, skipping test."
    api_json_response = response.json()
    assert "data" in api_json_response and "count" in api_json_response["data"], "API response missing count data."

    parsed_api_response = []
    for entry in api_json_response["data"]["count"]:
        timestamp = entry["timestamp"]
        for key, value in entry.items():
            if key != "timestamp":
                parsed_api_response.append([timestamp, key, value])

    logger_1.info(f"Parsed API Count Data: {parsed_api_response}")

    # Database Query for count data
    query = (
        f"""WITH LossData AS (SELECT DATEADD(DAY, DATEDIFF(DAY, 0, L.dtStamp), 0) AS timestamp, 
            ISNULL(L.lossType, '32') AS LossType, SUM(timeLost) / 60000 AS TimeLostMinutes, COUNT(*) AS Count 
            FROM Losses L WHERE L.startTime >= {start_timestamp} AND L.endTime <= {end_timestamp} 
            GROUP BY DATEADD(DAY, DATEDIFF(DAY, 0, L.dtStamp), 0), ISNULL(L.lossType, '32')) 
            SELECT CONVERT(VARCHAR, timestamp, 106) + ' 00:00:00:000' AS timestamp, LossType, 
            SUM(Count) AS TotalCount FROM LossData WHERE TimeLostMinutes > 0 
            GROUP BY timestamp, LossType ORDER BY timestamp, LossType;"""
    )

    database_data = fetch_data_from_database(query)
    assert database_data, "Database query returned None, skipping test."

    organized_database_data = [[entry["timestamp"], entry["LossType"], entry["TotalCount"]] for entry in database_data]

    logger_1.info(f"Organized Database Count Data: {organized_database_data}")

    assert compare_api_db_data(parsed_api_response, organized_database_data)


@pytest.mark.Loss_Analysis
def test_duration_response_with_database(common_payload_attributes, get_unix_timestamps):
    # Initialize API Client
    api_client = APIClient()
    # Load environment variables
    base_url = environment['base_url']

    logger_1.info(
        f"Running tests against environment: {base_url}, Project Name: {test_data.PROJECT_NAME}, "
        f"Project ID: {environment['project_id']}, NODE_ID: {test_data.NODE_IDs}, "
        f"Start_date:{test_data.START_TIME}, End_time:{test_data.END_TIME}"
    )

    start_timestamp, end_timestamp = get_unix_timestamps
    endpoint = '/externalUrlNew/loss_type_timeseries'
    payload = {
        "project_id": environment['project_id'],
        "start_time": test_data.START_TIME,
        "end_time": test_data.END_TIME,
        "selected_filters": "{\"shifts\":[],\"part variants\":[]}",
        "shift_to_shift": True
    }
    constructed_url = f"{base_url}{endpoint}"
    logger_1.info(f"Constructed API URL for duration data: {constructed_url}")

    response = api_client.session.post(
        constructed_url,
        json=payload,
        headers={"Authorization": f"Bearer {api_client.token}", "Content-Type": "application/json"}
    )
    logger_1.info(f"response content:{response.text}")
    assert response.status_code == 200, f"API returned unexpected status: {response.status_code}"
    assert response.content, "API returned no data, skipping test."

    api_json_response = response.json()
    assert "data" in api_json_response and "duration" in api_json_response[
        "data"], "API response missing duration data."

    parsed_api_response = []
    for entry in api_json_response["data"]["duration"]:
        timestamp = entry["timestamp"]
        for key, value in entry.items():
            if key != "timestamp":  # Ignore timestamp key
                parsed_api_response.append([timestamp, key, value])

    logger_1.info(f"Parsed API Duration Data: {parsed_api_response}")

    # Database Query for duration data (You need to update this query)
    query = (f"""WITH LossData AS
             (SELECT DATEADD(DAY, DATEDIFF(DAY, 0, L.dtStamp), 0) AS timestamp, 
             ISNULL(L.lossType, '32') AS LossType, 
             SUM(timeLost) / 60000 AS TimeLostMinutes, COUNT(*) AS Count 
             FROM Losses L 
             WHERE L.startTime >= {start_timestamp} AND L.endTime <= {end_timestamp} 
             GROUP BY DATEADD(DAY, DATEDIFF(DAY, 0, L.dtStamp), 0), ISNULL(L.lossType, '32')) 
             SELECT CONVERT(VARCHAR, timestamp, 106) + ' 00:00:00:000' AS timestamp, 
             LossType, 
             SUM(TimeLostMinutes) AS TotalTimeLostMinutes 
             FROM LossData 
             WHERE TimeLostMinutes > 0 
             GROUP BY timestamp, LossType 
             ORDER BY timestamp, LossType;""")

    database_data = fetch_data_from_database(query)
    assert database_data, "Database query returned None, skipping test."

    organized_database_data = [[entry["timestamp"], entry["LossType"], entry["TotalTimeLostMinutes"]] for entry in
                               database_data]

    logger_1.info(f"Organized Database Duration Data: {organized_database_data}")

    assert compare_api_db_data(parsed_api_response, organized_database_data)
