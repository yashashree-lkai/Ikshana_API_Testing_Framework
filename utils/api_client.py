import requests
from config.config_loader import environment
from config.logging_config import logger_1
from urllib.parse import unquote


def _log_response(response, action):
    """
    Logs the full details of an HTTP response.
    """
    logger_1.debug(f"{action} - URL: {unquote(response.request.url)}")
    if response.request.body:
        logger_1.debug(f"{action} - Request Body: {response.request.body}")
    logger_1.debug(f"{action} - Status Code: {response.status_code}")


class APIClient:
    def __init__(self):
        # Load environment-specific configurations
        self.base_url = environment['base_url']
        self.signin_url = f"{self.base_url}/signin"
        self.refresh_token_url = f"{self.base_url}/refreshToken"
        self.refresh_token = None
        self.token = None
        self.username = environment['username']
        self.password = environment['password']

        # Initialize a session object
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})

        # Perform sign-in
        self.signin()

    def signin(self):
        """
        Handles user sign-in and token initialization.
        """
        payload = {'user_name': self.username, 'password': self.password}
        response = self.session.post(self.signin_url, json=payload)
        _log_response(response, "Sign-in")
        if response.status_code == 200:
            data = response.json()
            self.token = data.get('accessToken')
            self.refresh_token = data.get('refreshToken')
            if not self.token or not self.refresh_token:
                logger_1.error("Sign-in failed: Missing tokens in response.")
                raise ValueError("Sign-in failed: Missing tokens in response.")
            self.session.headers.update({'Authorization': f'Bearer {self.token}'})
            logger_1.info("Sign-in successful, token obtained.")
        else:
            logger_1.error(f"Sign-in failed with status code: {response.status_code}")
            response.raise_for_status()

    def refresh_token(self):
        """
        Handles token refresh and updates session headers.
        """
        payload = {'refreshToken': self.refresh_token}
        response = self.session.post(self.refresh_token_url, json=payload)
        _log_response(response, "Token Refresh")
        if response.status_code == 200:
            data = response.json()
            self.token = data.get('accessToken')
            if not self.token:
                logger_1.error("Token refresh failed: Missing access token in response.")
                raise ValueError("Token refresh failed: Missing access token in response.")
            self.session.headers.update({'Authorization': f'Bearer {self.token}'})
            logger_1.info("Token refreshed successfully.")
        else:
            logger_1.error(f"Token refresh failed with status code: {response.status_code}")
            response.raise_for_status()

    def logout(self):
        """
        Handles user logout.
        """
        try:
            response = self.session.post(f'{self.base_url}/logout')
            _log_response(response, "Logout")
            if response.status_code == 200:
                logger_1.info("Logout successful.")
            else:
                logger_1.warning(f"Logout failed with status code: {response.status_code}")
            return response
        except requests.RequestException as e:
            logger_1.error(f"Logout request failed: {e}")
            raise

    # If reuired in future we will uncommnent this.
    # def get(self, endpoint, params=None):
    #     response = requests.get(f'{self.base_url}{endpoint}', headers=self.headers, params=params)
    #     if response.status_code == 401:
    #         print("Received 401, attempting to refresh token")
    #         self.refresh_token()
    #         response = requests.get(f'{self.base_url}{endpoint}', headers=self.headers)
    #     return response.json()
    #
    # def post(self, endpoint, json=None):
    #     response = requests.post(f'{self.base_url}{endpoint}', headers=self.headers, json=json)
    #     if response.status_code == 401:  # Unauthorized
    #         print("Received 401, attempting to refresh token")
    #         self.refresh_token()
    #         response = requests.post(f'{self.base_url}{endpoint}', headers=self.headers, json=json)
    #     return response
