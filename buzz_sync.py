import os
from dotenv import load_dotenv
import asyncio
import aiohttp
import time
import sys
import logging

# Custom logging formatter with timestamp, log level emoji, and message
class CustomFormatter(logging.Formatter):
    fmt = "%(asctime)s | %(levelname_emoji)s %(levelname)-8s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    LEVEL_EMOJIS = {
        logging.DEBUG: "🐞",
        logging.INFO: "📰",
        logging.WARNING: "⚠️",
        logging.ERROR: "❌",
        logging.CRITICAL: "🔥"
    }

    def format(self, record):
        record.levelname_emoji = self.LEVEL_EMOJIS.get(record.levelno, "")
        formatter = logging.Formatter(self.fmt, self.datefmt)
        return formatter.format(record)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Clear existing handlers
logger.handlers = []

# Add custom handler
handler = logging.StreamHandler()
handler.setFormatter(CustomFormatter())
logger.addHandler(handler)

# Function to retry API calls with exponential backoff
async def retry_api_call(func, *args, retries=5, backoff_in_seconds=1, **kwargs):
    for i in range(retries):
        try:
            return await func(*args, **kwargs)
        except (aiohttp.ClientError, aiohttp.ClientResponseError) as e:
            logger.error(f"API call failed: {e}. Retrying in {backoff_in_seconds} seconds...")
            await asyncio.sleep(backoff_in_seconds)
            backoff_in_seconds *= 2
    return None

# Function to get torrent hashes and selected files for a specific page
async def get_torrent_hashes_and_files_page(session, api_key, page):
    logger.info(f"Fetching torrent hashes and files for page {page}")
    try:
        url = f"https://api.real-debrid.com/rest/1.0/torrents?limit=50&page={page}"
        headers = {"Authorization": f"Bearer {api_key}"}
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                logger.error(f"Failed to fetch torrents: HTTP {response.status} for page {page}")
                return []

            response_json = await response.json()
            torrents = []
            for torrent in response_json:
                if isinstance(torrent, dict):
                    torrent_id = torrent['id']
                    files_url = f"https://api.real-debrid.com/rest/1.0/torrents/info/{torrent_id}"
                    async with session.get(files_url, headers=headers) as files_response:
                        if files_response.status == 200:
                            files_info = await files_response.json()
                            selected_files = [file['id'] for file in files_info.get('files', []) if file['selected']]
                            torrents.append((torrent['hash'], selected_files))
                            logger.info(f"Fetched {len(selected_files)} selected files for torrent {torrent_id}")
                        else:
                            logger.error(f"Failed to fetch files for torrent {torrent_id}: HTTP {files_response.status}")
            return torrents
    except aiohttp.ClientError as e:
        logger.error(f"Request failed: {e}. Page: {page}")
        return []

# Function to get all torrent hashes and selected files concurrently
async def get_all_torrent_hashes_and_files(session, api_key):
    torrents = []
    page = 1

    # First, determine the number of pages
    try:
        url = f"https://api.real-debrid.com/rest/1.0/torrents?limit=50&page={page}"
        headers = {"Authorization": f"Bearer {api_key}"}
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                logger.error(f"Initial request failed: HTTP {response.status}")
                return []
            total_torrents = int(response.headers.get('X-Total-Count', 0))
            total_pages = (total_torrents // 50) + 1
            logger.info(f"Total torrents: {total_torrents}, Total pages: {total_pages}")
    except (aiohttp.ClientError, ValueError) as e:
        logger.error(f"Initial request failed: {e}")
        return []

    tasks = [get_torrent_hashes_and_files_page(session, api_key, page) for page in range(1, total_pages + 1)]
    results = await asyncio.gather(*tasks)
    for result in results:
        if result:
            torrents.extend(result)
    return torrents

# Function to fetch all existing torrents for a secondary account
async def fetch_existing_torrents(session, api_key):
    torrents = []
    page = 1

    while True:
        url = f"https://api.real-debrid.com/rest/1.0/torrents?limit=50&page={page}"
        headers = {"Authorization": f"Bearer {api_key}"}
        async with session.get(url, headers=headers) as response:
            if response.status == 204:  # No Content
                break
            if response.status != 200:
                logger.error(f"Failed to fetch existing torrents for page {page}: HTTP {response.status}")
                return []

            response_json = await response.json()
            if isinstance(response_json, list) and not response_json:
                break

            torrents.extend(response_json)
            page += 1

            if len(response_json) < 50:
                break

    return torrents

# Function to add or update a torrent by its hash and select specified files
async def add_or_update_torrent_and_select_files(session, api_key, torrent_hash, file_ids):
    if dry_run:
        logger.info(f"Dry run enabled. Skipping adding or updating hash {torrent_hash}.")
        return None

    headers = {"Authorization": f"Bearer {api_key}"}
    magnet_link = f"magnet:?xt=urn:btih:{torrent_hash}"
    try:
        # Check if the torrent already exists
        logger.info(f"Checking if torrent {torrent_hash} already exists...")
        existing_torrents = await fetch_existing_torrents(session, api_key)
        existing_torrent_ids = [t['hash'] for t in existing_torrents]
        if torrent_hash in existing_torrent_ids:
            logger.info(f"Torrent {torrent_hash} already exists. Checking selected files...")

            # Fetch existing torrent info
            existing_torrent = next(t for t in existing_torrents if t['hash'] == torrent_hash)
            existing_torrent_id = existing_torrent['id']
            files_url = f"https://api.real-debrid.com/rest/1.0/torrents/info/{existing_torrent_id}"
            async with session.get(files_url, headers=headers) as existing_files_response:
                if existing_files_response.status == 200:
                    existing_files_info = await existing_files_response.json()
                    existing_selected_files = [file['id'] for file in existing_files_info.get('files', []) if file['selected']]

                    # If the selected files differ, update them
                    if set(existing_selected_files) != set(file_ids):
                        logger.info(f"Updating selected files for torrent {torrent_hash}...")
                        select_url = f"https://api.real-debrid.com/rest/1.0/torrents/selectFiles/{existing_torrent_id}"
                        async with session.post(select_url, headers=headers, data={"files": ",".join(map(str, file_ids))}) as response:
                            if response.status not in [204, 200]:
                                logger.error(f"Failed to update selected files: HTTP {response.status}")
                                return None
                    else:
                        logger.info(f"Selected files for torrent {torrent_hash} are already up-to-date.")
                else:
                    logger.error(f"Failed to fetch files for existing torrent {existing_torrent_id}: HTTP {existing_files_response.status}")
        else:
            logger.info(f"Adding new torrent {torrent_hash}...")

            # Add the torrent
            add_url = "https://api.real-debrid.com/rest/1.0/torrents/addMagnet"
            async with session.post(add_url, headers=headers, data={"magnet": magnet_link}) as response:
                if response.status != 201:
                    logger.error(f"Failed to add torrent: HTTP {response.status}")
                    return None

                torrent_id = (await response.json()).get('id')
                if not torrent_id:
                    logger.error(f"Failed to get torrent ID after adding torrent: {await response.text()}")
                    return None

                # Delay to ensure the torrent is fully processed
                await asyncio.sleep(5)

                # Select files to download
                logger.info(f"Selecting files for new torrent {torrent_id}...")
                select_url = f"https://api.real-debrid.com/rest/1.0/torrents/selectFiles/{torrent_id}"
                async with session.post(select_url, headers=headers, data={"files": ",".join(map(str, file_ids))}) as response:
                    if response.status not in [204, 200]:
                        logger.error(f"Failed to select files: HTTP {response.status}")
                        return None

        return torrent_id
    except aiohttp.ClientError as e:
        logger.error(f"Request failed: {e}")
        return None

# Function to sync primary account with secondary accounts
async def sync_accounts(primary_api_key, secondary_api_keys):
    async with aiohttp.ClientSession() as session:
        # Get all hashes and selected files from primary account
        logger.info("Fetching all torrent hashes and selected files from primary account...")
        primary_torrents = await get_all_torrent_hashes_and_files(session, primary_api_key)
        logger.info(f"Primary account has {len(primary_torrents)} torrents.")

        for api_key in secondary_api_keys:
            logger.info(f"Syncing with secondary account {api_key}...")
            secondary_existing_torrents = await fetch_existing_torrents(session, api_key)
            secondary_hashes = {torrent['hash'] for torrent in secondary_existing_torrents}

            tasks = []
            for torrent_hash, file_ids in primary_torrents:
                if torrent_hash not in secondary_hashes:
                    logger.info(f"Creating task to add/update torrent {torrent_hash} in secondary account {api_key}...")
                    tasks.append(add_or_update_torrent_and_select_files(session, api_key, torrent_hash, file_ids))

            logger.info(f"Running {len(tasks)} tasks to sync torrents with secondary account {api_key}...")
            results = await asyncio.gather(*tasks)
            for result, (torrent_hash, _) in zip(results, primary_torrents):
                if result:
                    logger.info(f"Added or updated hash {torrent_hash} successfully and selected files.")
                else:
                    logger.error(f"Failed to add or update hash {torrent_hash}.")

# Check for --dry-run flag
dry_run = '--dry-run' in sys.argv
if dry_run:
    logger.info("Dry run enabled. No changes will be made.")

# Load environment variables from .env file
if not os.path.exists('.env'):
    logger.error("Please create a .env file with the following contents:")
    logger.error("RD_PRIMARY_API_KEY=your_primary_api_key")
    logger.error("RD_SECONDARY_API_KEYS=your_secondary_api_key1,your_secondary_api_key2")
    sys.exit(1)

load_dotenv()

# Load primary and secondary API keys from environment variables
primary_api_key = os.getenv('RD_PRIMARY_API_KEY')
secondary_api_keys = os.getenv('RD_SECONDARY_API_KEYS')

# Validate environment variables
if primary_api_key is None:
    logger.error("RD_PRIMARY_API_KEY is not set in the .env file.")
    sys.exit(1)

if secondary_api_keys is None:
    logger.error("RD_SECONDARY_API_KEYS is not set in the .env file.")
    sys.exit(1)

# Split secondary API keys
secondary_api_keys = secondary_api_keys.split(',')

# Here be dragons
asyncio.run(sync_accounts(primary_api_key, secondary_api_keys))
