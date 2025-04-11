import requests
from config.config_loader import environment
from config.logging_config import logger_1


def get_auth_token():
    """
    Fetch authentication token for the specified environment.
    """

    signin_url = f"{environment['base_url']}/signin"
    headers = {'Content-Type': 'application/json',  'charset': 'utf-8'}
    payload = {'user_name': environment['username'], 'password': environment['password']}

    response = requests.post(signin_url, json=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        logger_1.info("Authentication successful, token obtained.")
        return {
            'access_token': data['accessToken'],
            'refresh_token': data['refreshToken'],
            'headers': {
                'Content-Type': 'application/json', 'charset': 'utf-8',
                'Authorization': f"Bearer {data['accessToken']}"
            }
        }
    else:
        logger_1.error(f"Authentication failed: {response.status_code}, {response.json()}")
        raise Exception(f"Authentication failed: {response.status_code}, {response.json()}")

