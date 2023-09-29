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
                for _ in range(500000):
                    prefix = 'did:plc'
                    alphanumeric = string.ascii_letters + string.digits
                    suffix = ''.join(random.choices(alphanumeric + '._:%-', k=10))

                    did = prefix + suffix
                    random_string = ''.join(random.choices(string.ascii_letters, k=6))
                    handle = random_string + '.bsky.social'
                    status = random.choice([True, False])  # Random status

                    user_data.append((did, handle, status))

                await connection.executemany(
                    "INSERT INTO users (did, handle, status) VALUES ($1, $2, $3)",
                    user_data
                )

                return user_data
    except Exception as e:
        logger.error(f"Error generating random data: {e}")


async def generate_random_block_data(user_data):
    try:
        async with database_handler.connection_pool.acquire() as connection:
            async with connection.transaction():
                # Generate and insert random blocklist data into the 'blocklists' table
                blocklists_data = []
                for _ in range(1000000):
                    user_did = random.choice([user[0] for user in user_data])  # Random user DID
                    blocked_did = random.choice([user[0] for user in user_data])  # Random blocked DID
                    block_date = generate_random_date()  # Random block date

                    blocklists_data.append((user_did, blocked_did, block_date))

                await connection.executemany(
                    "INSERT INTO blocklists (user_did, blocked_did, block_date) VALUES ($1, $2, $3)",
                    blocklists_data
                )

    except Exception as e:
        logger.error(f"Error generating random data: {e}")


# Function to generate a random date within a specified range
def generate_random_date():
    start_date = datetime.date(2022, 1, 1)  # Change this to your desired start date
    end_date = datetime.date(2023, 9, 15)  # Change this to your desired end date

    random_date = start_date + datetime.timedelta(days=random.randint(0, (end_date - start_date).days))
    return random_date.isoformat()
