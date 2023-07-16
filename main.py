import requests
import urllib.parse
import dns.resolver
from datetime import datetime
from flask import Flask, render_template, request
# ======================================================================================================================
# **************************************************** Functions *******************************************************
# ======================================================================================================================
# url = "https://bsky.social/xrpc/com.atproto.repo.listRecords?repo=did:plc:w4xbfzo7kqfes5zb7r6qv3rw&collection=app.bsky.graph.block"


def get_block_list():
    repo = input("Enter DID or Handle: ")
    base_url = "https://bsky.social/xrpc/"
    collection = "app.bsky.graph.block"

    url = urllib.parse.urljoin(base_url, "com.atproto.repo.listRecords")
    params = {
        "repo": repo,
        "collection": collection
    }

    encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    full_url = f"{url}?{encoded_params}"

    # print(full_url)  # Shows full URL of search
    response = requests.get(full_url)

    if response.status_code == 200:
        blocked_users = []
        response_json = response.json()
        records = response_json["records"]
        for record in records:
            subject = record["value"]["subject"]
            blocked_users.append(subject)
        if not blocked_users:
            print(repo + ": hasn't blocked anyone.")
        else:
            return blocked_users
    else:
        print("Error:", response.status_code)


# get blocks that a user has made
def get_user_block_list(ident):
    # repo = input("Enter DID or Handle: ")
    base_url = "https://bsky.social/xrpc/"
    # repo = "did:plc:w4xbfzo7kqfes5zb7r6qv3rw"
    collection = "app.bsky.graph.block"

    url = urllib.parse.urljoin(base_url, "com.atproto.repo.listRecords")
    params = {
        "repo": ident,
        "collection": collection
    }

    encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    full_url = f"{url}?{encoded_params}"

    print(full_url)  # Shows full URL of search
    response = requests.get(full_url)

    if response.status_code == 200:
        blocked_users = []
        created_dates = []
        response_json = response.json()
        records = response_json.get("records", [])
        for record in records:
            value = record.get("value", {})
            subject = value.get("subject")
            created_at_value = value.get("createdAt")
            if subject:
                blocked_users.append(subject)
            if created_at_value:
                created_date = datetime.strptime(created_at_value, "%Y-%m-%dT%H:%M:%S.%fZ").date()
                created_dates.append(created_date)

        if not blocked_users:
            print(ident + ": hasn't blocked anyone.")

        # Return the blocked users and created_at timestamps if needed
        return blocked_users, created_dates


# Uses DNS to get DID
def get_did_from_handle():
    handle = input("Enter handle: ")
    txt_record = "_atproto."
    result = dns.resolver.resolve(txt_record + handle, "TXT")
    for server in result:
        for txt_string in server.strings:
            did = txt_string.decode("utf-8")
            if did.startswith("did="):
                did = did[len("did="):]
                return did


# Uses ATP to get DID
def resolve_handle(info):
    base_url = "https://bsky.social/xrpc/"
    url = urllib.parse.urljoin(base_url, "com.atproto.identity.resolveHandle")
    params = {
        "handle": info
    }

    encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    full_url = f"{url}?{encoded_params}"

    # print(full_url) # Shows full URL of search
    get_response = requests.get(full_url)
    response = get_response.json().values()
    return list(response)[0]


# Get handle of DID
def resolve_did(did):
    handle = did
    base_url = "https://bsky.social/xrpc/"
    url = urllib.parse.urljoin(base_url, "com.atproto.repo.describeRepo")
    params = {
        "repo": handle
    }

    encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    full_url = f"{url}?{encoded_params}"

    # print(full_url)  # Shows full URL of search
    get_response = requests.get(full_url)

    if get_response.status_code == 200:
        response_json = get_response.json()
        records = response_json["handle"]
        return records
    else:
        print("Error:", get_response.status_code)


def process_did_list_to_handle(did_list):
    handle_list = []
    for item in did_list:
        handle_list.append(resolve_did(item))
    return handle_list


def blocklist():
    ident = request.form['ident']
    blocked_users, created_dates = get_user_block_list(ident)
    return render_template('blocklist.html', blocked_users=blocked_users, created_dates=created_dates)


# ======================================================================================================================
# **************************************************** CORE CODE *******************************************************
# ======================================================================================================================
identifier = input("Enter DID or handle: ")
selection = input("What would you like to do?\n 1. Get DID\n 2. Get Handle\n 3. Get Block list of a user\n > ")

if selection == "1":
    print(resolve_handle(identifier))
elif selection == "2":
    print(resolve_did(identifier))
elif selection == "3":
    print(identifier + " has blocked: \n")
    blocked_users, timestamps = get_user_block_list(identifier)
    try:
        for user, timestamp in zip(process_did_list_to_handle(blocked_users), timestamps):
            print(f"Handle: {user}\tBlocked on: {timestamp}")
    except TypeError:
        pass
else:
    print("Invalid entry.")
