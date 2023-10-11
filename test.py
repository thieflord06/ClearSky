# test.py

from config_helper import logger
import database_handler
import random
import string
import datetime


async def generate_random_user_data():
    try:
        async with database_handler.connection_pool.acquire() as connection:
            async with connection.transaction():
                # Generate and insert random user data into the 'users' table
                user_data = []
                for _ in range(50000):
                    prefix = 'did:plc:'
                    alphanumeric = string.ascii_letters + string.digits
                    suffix = ''.join(random.choices(alphanumeric + '._:%-', k=24))

                    did = prefix + suffix
                    random_string = ''.join(random.choices(string.ascii_letters, k=10))
                    handle = random_string + '.bsky.social'
                    status = random.choice([True, False])  # Random status

                    user_data.append((did, handle, status))

                await connection.executemany(
                    "INSERT INTO users (did, handle, status) VALUES ($1, $2, $3)",
                    user_data
                )

                logger.info("user data created.")

                return user_data
    except Exception as e:
        logger.error(f"Error generating random data: {e}")


async def generate_random_block_data(user_data):
    try:
        async with database_handler.connection_pool.acquire() as connection:
            async with connection.transaction():
                # Generate and insert random blocklist data into the 'blocklists' table

                blocklists_data = []

                for user in user_data:
                    user_did = user[0]  # User DID
                    num_blocked_dids = random.randint(0, 150)  # Random number of blocked_dids (adjust the range as needed)

                    blocked_dids = random.sample([user[0] for user in user_data if user[0] != user_did], num_blocked_dids)
                    block_dates = [generate_random_date() for _ in range(num_blocked_dids)]

                    # Create blocklist entries for this user
                    for blocked_did, block_date in zip(blocked_dids, block_dates):
                        blocklists_data.append((user_did, blocked_did, block_date))

                await connection.executemany(
                    "INSERT INTO blocklists (user_did, blocked_did, block_date) VALUES ($1, $2, $3)",
                    blocklists_data
                )

                logger.info("blocklist data created.")

    except Exception as e:
        logger.error(f"Error generating random data: {e}")


# Function to generate a random date within a specified range
def generate_random_date():
    current_date = datetime.date.today()
    start_date = datetime.date(2022, 1, 1)  # Change this to your desired start date
    end_date = current_date  # Change this to your desired end date
    random_date = start_date + datetime.timedelta(days=random.randint(0, (end_date - start_date).days))

    return random_date.isoformat()
