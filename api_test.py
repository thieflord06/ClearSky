# api_test.py

from config_helper import logger
import requests


def main():
    api_key = "CLEARSKYtest121ascmkdneaorSDno32"

    # api_endpoint = "https://staging.bsky.thieflord.dev/api/v1/base/internal/status/process-status"
    # api_endpoint = "http://localhost/api/v1/base/internal/status/process-status"
    # api_endpoint = "http://staging.bsky.thieflord.dev/api/v1/blocklist-search-blocking/test.tennis.thieflord.dev"
    # api_endpoint = "http://localhost/api/v1/lists/block-stats"
    api_endpoint = "http://staging.bsky.thieflord.dev/api/v1/lists/funer-facts"
    # api_endpoint = "https://staging.bsky.thieflord.dev/api/v1/blocklist-search-blocking/test.tennis.thieflord.dev/alechiaval.bsky.social"
    # api_endpoint = "https://staging.bsky.thieflord.dev/api/v1/lists/block-stats"
    # api_endpoint = "https://staging.bsky.thieflord.dev"

    # Define the headers with the API key
    headers = {'X-API-Key': f'{api_key}'}
    try:
        # Send a GET request to the API endpoint with the headers
        response = requests.get(api_endpoint, headers=headers)

        # Print the response headers
        for key, value in response.headers.items():
            logger.info(f"Header: {key} = {value}")

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Print the response content
            logger.info(response.text)
        else:
            logger.info(f'Request failed with status code {response.status_code}')
    except requests.exceptions.RequestException as e:
        logger.info(f'Error: {e}')


if __name__ == '__main__':
    main()
