import json
import pytest
from database_operations import fetch_data_from_database
from config.config_loader import environment
from data.test_data import test_data
from utils.api_client import APIClient
from config.logging_config import logger_1
from conftest import compare_api_db_data


@pytest.mark.Loss_Analysis
def test_count_response_with_database(common_payload_attributes, get_unix_timestamps):
    api_client = APIClient()
    base_url = environment['base_url']

    logger_1.info(
        f"Running tests against environment: {base_url}, Project Name: {test_data.PROJECT_NAME}, "
        f"Project ID: {environment['project_id']}, Start_timestamp:{test_data.START_TIME}, End_timestamp:{test_data.END_TIME}"
    )

    start_timestamp, end_timestamp = get_unix_timestamps

    endpoint = '/externalUrlNew/cause_of_loss_pareto'
    payload = {
        "project_id": environment['project_id'],
        "start_time": test_data.START_TIME,
        "end_time": test_data.END_TIME,
        "selected_filters": json.dumps({"part variants": [], "shifts": []})
    }
    constructed_url = f"{base_url}{endpoint}"
    logger_1.info(f"Constructed API URL: {constructed_url}")

    response = api_client.session.post(
        constructed_url,
        json=payload,
        headers={"Authorization": f"Bearer {api_client.token}", "Content-Type": "application/json"}
    )
    assert response.status_code == 200, f"API returned unexpected status: {response.status_code}"
    assert response.content, "API returned no data, skipping test."

    api_json_response = response.json()
    assert api_json_response.get("data"), "API returned empty or invalid data."
    parsed_api_response_count = [[entry["causeName"], entry["count"], entry["cumulative_percentage"]] for entry in
                                 api_json_response["data"]["count"]]
    logger_1.info(f"Parsed API Response for Count: {parsed_api_response_count}")

    parsed_api_response_duration = [[entry["causeName"], entry["duration"], entry["cumulative_percentage"]] for entry in
                                    api_json_response["data"]["duration"]]
    logger_1.info(f"Parsed API Response for Duration: {parsed_api_response_duration}")

    count_query = (
        f"""WITH total_count AS (SELECT COUNT(*) AS totalCount FROM Losses WHERE endTime >= {start_timestamp} AND 
        startTime <= {end_timestamp}),
        loss_data AS (SELECT CASE WHEN l.isAttributed = 0 THEN 'Unattributed Losses' ELSE lc.causeName END AS 
        causeName, COUNT(*) AS total_count
        FROM Losses l LEFT JOIN LossCauses lc ON l.causeOfLoss = lc.causeId 
        WHERE endTime >= {start_timestamp} AND startTime <= {end_timestamp} 
        GROUP BY CASE WHEN l.isAttributed = 0 THEN 'Unattributed Losses' ELSE lc.causeName END),
        cumulative_data AS (SELECT causeName, total_count, SUM(total_count) OVER (ORDER BY total_count DESC) AS 
        cumulative_count FROM loss_data)
        SELECT causeName, total_count,  CAST(
        ROUND((cumulative_count * 100.0) / (SELECT totalCount FROM total_count), 2) AS FLOAT)
        AS cumulative_percentage FROM cumulative_data ORDER BY total_count DESC;""")

    database_data = fetch_data_from_database(count_query)
    assert database_data, "Database query returned None, skipping test."

    organized_database_data_count = [[entry["causeName"], entry["total_count"], entry["cumulative_percentage"]]
                                     for entry in database_data]

    logger_1.info(f"Organized Database Data for count: {organized_database_data_count}")

    duration_query = (
        f"""WITH total_time AS (SELECT SUM(CAST(timeLost AS numeric(18,2)))/60000 AS totalTimeLost FROM Losses WHERE 
           endTime >= {start_timestamp} AND startTime <= {end_timestamp}),
           loss_data AS (SELECT CASE WHEN l.isAttributed = 0 THEN 'Unattributed Losses' ELSE lc.causeName END AS 
           causeName, SUM(CAST(timeLost AS numeric(18,2)))/60000 AS duration
           FROM Losses l LEFT JOIN LossCauses lc ON l.causeOfLoss = lc.causeId 
           WHERE endTime >= {start_timestamp} AND startTime <= {end_timestamp} 
           GROUP BY CASE WHEN l.isAttributed = 0 THEN 'Unattributed Losses' ELSE lc.causeName END), 
           cumulative_data AS (SELECT causeName, duration, SUM(duration) OVER (ORDER BY duration DESC) AS 
           cumulative_duration FROM loss_data)
           SELECT causeName, duration, (CAST(cumulative_duration AS numeric(18,2)) / (SELECT totalTimeLost FROM 
           total_time)) * 100 AS cumulative_percentage
           FROM cumulative_data ORDER BY duration DESC;"""
    )
    database_data = fetch_data_from_database(duration_query)
    # Execute the database query and organize results
    assert database_data, "Database query returned None, skipping test."
    organized_database_data_duration = [[entry["causeName"], int(entry["duration"]), int(entry["cumulative_percentage"])]
                                        for entry in database_data]

    logger_1.info(f"Organized Database Data for Duration: {organized_database_data_duration}")

    errors = []

    try:
        assert compare_api_db_data(parsed_api_response_count, organized_database_data_count), "Count comparison failed."
        logger_1.info("Count comparison successful.")
    except AssertionError as e:
        logger_1.error(f"Count comparison failed: {e}")
        errors.append(f"Count comparison failed: {e}")

    try:
        assert compare_api_db_data(parsed_api_response_duration,
                                   organized_database_data_duration), "Duration comparison failed."
        logger_1.info("Duration comparison successful.")
    except AssertionError as e:
        logger_1.error(f"Duration comparison failed: {e}")
        errors.append(f"Duration comparison failed: {e}")

    if errors:
        pytest.fail("/n".join(errors))
