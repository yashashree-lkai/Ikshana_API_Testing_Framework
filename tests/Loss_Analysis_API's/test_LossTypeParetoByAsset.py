import json
import pytest
from database_operations import fetch_data_from_database
from config.environment import  environment
from data.test_data import test_data
from utils.api_clint import APIClient
from config.logging_config import logger_1
from conftest import compare_api_db_data


@pytest.mark.Loss_Analysis
def test_count_response_with_database(common_payload_attributes, get_unix_timestamps):
    api_client = APIClient()
    base_url = environment['base_url']
    project_name = test_data.PROJECT_NAME
    config_id = environment['config_id']
    project_id = environment['project_id']
    start_timestamp, end_timestamp = get_unix_timestamps

    logger_1.info(
        f"Running tests against environment: {base_url}, Project Name: {project_name}, "
        f"Project ID: {project_id}, Start_timestamp:{start_timestamp}, End_timestamp:{end_timestamp}"
    )

    endpoint = '/externalUrlNew/loss_type_pareto_by_asset'
    payload = {
        "project_id": project_id,
        "start_time": test_data.START_TIME,
        "end_time": test_data.END_TIME,
        "selected_filters": json.dumps({"shifts": [], "part variants": []})
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
    assert api_json_response.get("data"), "API returned empty or invalid data."
    parsed_api_response = [[entry["loss_type"], entry["count"], entry["cumulative_percentage"]] for entry in
                           api_json_response["data"]["count"]]
    logger_1.info(f"Parsed API Response: {parsed_api_response}")

    query = (
        f"""WITH LossData AS (SELECT ISNULL(lossType, '32') AS LossType, SUM(timeLost)/60000 AS TimeLost,
        COUNT(*) AS Count FROM Losses L where isAttributed = 0 AND 
        startTime>= {start_timestamp} AND endTime <= {end_timestamp} GROUP BY lossType 
        Union ALL SELECT lossType AS LossType,SUM(timeLost)/60000 AS TimeLost,COUNT(*) AS 
        Count FROM Losses L, LossTypes LT WHERE L.lossType = LT.lossTypeId AND 
        startTime >= {start_timestamp} AND endTime <= {end_timestamp} AND isAttributed = 1 GROUP BY lossType), 
        LossCount AS (SELECT LossType,Count, TimeLost, SUM(Count) OVER (ORDER BY Count DESC) AS CumulativeCount,
        SUM(TimeLost) OVER (ORDER BY Count DESC) AS CumulativeTimeLost, SUM(Count) OVER () AS TotalCount, 
        SUM(TimeLost) OVER () AS TotalTimeLost FROM LossData) SELECT LossType, Count, 
        ROUND(CumulativeCount * 100/ TotalCount, 0) AS CumulativePercentageCount FROM  LossCount
         WHERE TimeLost > 0 ORDER BY Count DESC;"""
    )

    database_data = fetch_data_from_database(query)
    assert database_data, "Database query returned None, skipping test."

    organized_database_data = [[entry["LossType"], entry["Count"], entry["CumulativePercentageCount"]]
                               for entry in database_data]

    logger_1.info(f"Organized Database Data: {organized_database_data}")

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
    endpoint = '/externalUrlNew/loss_type_pareto_by_asset'
    payload = {
        "project_id": environment['project_id'],
        "start_time": test_data.START_TIME,
        "end_time": test_data.END_TIME,
        "selected_filters": json.dumps({"shifts": [], "part variants": []})
    }
    # Make the API request and parse the response
    constructed_url = f"{base_url}{endpoint}"
    logger_1.info(f"Constructed API URL: {constructed_url}")

    response = api_client.session.post(
        constructed_url,
        json=payload,
        headers={"Authorization": f"Bearer {api_client.token}", "Content-Type": "application/json"}
    )
    logger_1.info(f"response content:{response.content}")
    assert response.status_code == 200, f"API returned unexpected status: {response.status_code}"
    assert response.content, "API returned no data, skipping test."

    api_json_response = response.json()
    assert api_json_response.get("data"), "API returned empty or invalid data."
    parsed_api_response = [[entry["loss_type"], entry["duration"], entry["cumulative_percentage"]] for entry in
                           api_json_response["data"]["duration"]]
    logger_1.info(f"Parsed API Response: {parsed_api_response}")

    query = (
        f"""WITH LossData AS (SELECT ISNULL(lossType, '32') AS LossType, SUM(cast(timeLost as decimal(18,2)))/60000 
        AS TimeLost, COUNT(*) AS Count FROM Losses L where isAttributed = 0 
        AND startTime>= {start_timestamp} AND endTime <= {end_timestamp} 
        GROUP BY lossType Union ALL SELECT lossType AS LossType,SUM(cast(timeLost as decimal(18,2)))/60000 
        AS TimeLost,COUNT(*) AS Count FROM Losses L, LossTypes LT WHERE L.lossType = LT.lossTypeId 
        AND startTime >= {start_timestamp} AND endTime <= {end_timestamp} AND isAttributed = 1 
        GROUP BY lossType), LossCount AS (SELECT LossType,Count, cast(TimeLost as decimal(18,2)) 
        as TimeLost, SUM(Count) OVER (ORDER BY Count DESC) AS CumulativeCount, SUM(cast(TimeLost 
        as decimal(18,2))) OVER (ORDER BY Count DESC) AS CumulativeTimeLost, SUM(Count) OVER () 
        AS TotalCount, SUM(cast(TimeLost as decimal(18,2))) OVER () AS TotalTimeLost FROM LossData) 
        SELECT LossType, TimeLost AS TimeLost, ROUND(CumulativeTimeLost * 100/TotalTimeLost, 0) 
        AS CumulativePercentageTimeLost FROM  LossCount WHERE TimeLost > 0 ORDER BY Count DESC; """)

    database_data = fetch_data_from_database(query)
    # Execute the database query and organize results
    assert database_data, "Database query returned None, skipping test."
    organized_database_data = [[entry["LossType"], int(entry["TimeLost"]), int(entry["CumulativePercentageTimeLost"])]
                               for entry in database_data]

    logger_1.info(f"Organized Database Data: {organized_database_data}")

    # Compare API and database data
    assert compare_api_db_data(parsed_api_response, organized_database_data)
