# api_test.py

from config_helper import logger
import requests


def main():
    api_key = "CLEARSKYtest121ascmkdneaorSDno32"

    # api_endpoint = "https://staging.bsky.thieflord.dev/api/v1/base/internal/status/process-status"
    # api_endpoint = "http://localhost/api/v1/base/internal/status/process-status"
    # api_endpoint = "http://staging.bsky.thieflord.dev/api/v1/blocklist-search-blocking/test.tennis.thieflord.dev"
    # api_endpoint = "http://localhost/api/v1/lists/block-stats"
    # api_endpoint = "http://staging.bsky.thieflord.dev/api/v1/lists/fun-facts"
    # api_endpoint = "https://staging.bsky.thieflord.dev/api/v1/blocklist-search-blocking/test.tennis.thieflord.dev/alechiaval.bsky.social"
    # api_endpoint = "https://staging.bsky.thieflord.dev/api/v1/lists/block-stats"
    # api_endpoint = "https://staging.bsky.thieflord.dev/api/v1/blocklist/boykisser.expert"
    # api_endpoint = "https://staging.bsky.thieflord.dev/api/v1/single-blocklist/thieflord.dev"
    # api_endpoint = "https://staging.bsky.thieflord.dev/api/v1/total-users"
    # api_endpoint = "https://staging.bsky.thieflord.dev/api/v1/get-did/thieflord.dev"
    # api_endpoint = "https://staging.bsky.thieflord.dev/api/v1/get-handle/thieflford.dev"
    # api_endpoint = "https://staging.bsky.thieflord.dev/api/v1/get-handle-history/thieflofgrd.dev"
    # api_endpoint = "https://staging.bsky.thieflord.dev/api/v1/blocklist-search-blocking/desir.ee/thieflord.dev"
    # api_endpoint = "https://staging.bsky.thieflord.dev/api/v1/in-common-blocklist/"
    # api_endpoint = "http://localhost/api/v1/blocklist-search-blocking/desir.ee/thieflord.dev"
    # api_endpoint = "http://localhost/api/v1/blocklist-search-blocked/thieflord.dev/desir.ee"
    # api_endpoint = "https://staging.bsky.thieflord.dev/api/v1/total-users"
    # api_endpoint = "http://localhost/api/v1/single-blocklist/thieflord.dev"
    api_endpoint = "http://localhost/api/v1/at-uri/at://did:plc:smcanwhzsj5dqp4yew7y6ybx/app.bsky.graph.listblock/3kf2kfcic5od2u"

    # Define the headers with the API key
    headers = {'X-API-Key': f'{api_key}'}
    try:
        # Send an OPTIONS request to the API endpoint with the headers
        options_response = requests.options(api_endpoint, headers=headers)

        for key, value in options_response.headers.items():
            logger.info(f"Header: {key} = {value}")

        # Send a GET request to the API endpoint with the headers
        response = requests.get(api_endpoint, headers=headers)

        # Print the response headers
        for key, value in response.headers.items():
            logger.info(f"Header: {key} = {value}")

        # Check if the 'Access-Control-Allow-Origin' header is present
        if 'Access-Control-Allow-Origin' in response.headers:
            # Print the allowed origin(s)
            logger.info(f"Access-Control-Allow-Origin: {response.headers['Access-Control-Allow-Origin']}")

            # Check if it allows the origin of your request
            if response.headers['Access-Control-Allow-Origin'] == '*':
                logger.info("CORS is configured to allow all origins.")
            else:
                logger.info("CORS is configured to allow specific origins.")
        else:
            logger.info("Access-Control-Allow-Origin header not found. CORS might not be configured.")

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
