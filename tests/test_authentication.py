import pytest
import requests
from config.config_loader import environment
from config.logging_config import logger_1
from utils.auth_token_helper import get_auth_token


def test_sign_in():
    """
    Test valid authentication and token retrieval.
    """
    # Load environment configuration
    signin_url = f"{environment['base_url']}/signin"
    headers = {'Content-Type': 'application/json', 'charset': 'utf-8'}
    payload = {'user_name': environment['username'], 'password': environment['password']}

    # Send POST request to sign in
    response = requests.post(signin_url, json=payload, headers=headers)

    # Validate response
    assert response.status_code == 200, f"Expected 200, but got {response.status_code}"
    data = response.json()
    assert 'accessToken' in data, "accessToken not found in response."
    assert 'refreshToken' in data, "refreshToken not found in response."

    # Log success
    logger_1.info("Valid authentication test passed successfully.")


def test_invalid_authentication():
    """
    Test invalid authentication with wrong credentials.
    """
    # Load environment configuration
    signin_url = f"{environment['base_url']}/signin"
    headers = {'Content-Type': 'application/json'}
    payload = {'user_name': 'wrong_user', 'password': 'wrong_password'}

    # Send POST request to sign in
    response = requests.post(signin_url, json=payload, headers=headers)

    # Log response for debugging
    logger_1.info(f"Response Code: {response.status_code}, Response Body: {response.json()}")

    # Validate response status or error message
    if response.status_code == 200:
        data = response.json()
        assert data['error'] == 'username is incorrect', f"Unexpected error message: {data}"
    else:
        assert response.status_code == 401, f"Expected 401, but got {response.status_code}"

    # Log success
    logger_1.info("Invalid authentication test passed successfully.")


def test_token_refresh():
    """
    Test refreshing authentication token.
    """
    # Get valid auth token first
    tokens = get_auth_token()
    refresh_token = tokens['refresh_token']

    # Attempt to refresh token
    refresh_token_url = f"{environment['base_url']}/refreshToken"
    headers = {'Content-Type': 'application/json'}
    payload = {'refreshToken': refresh_token, 'lineId': environment['config_id']}

    response = requests.post(refresh_token_url, json=payload, headers=headers)

    # Validate response
    assert response.status_code == 200, f"Expected 200, but got {response.status_code}"
    data = response.json()
    assert 'accessToken' in data, "accessToken not found in response."

    # Log success
    logger_1.info("Token refresh test passed successfully.")

