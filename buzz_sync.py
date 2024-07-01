import os
import sys
from dotenv import load_dotenv
import asyncio
import aiohttp
from loguru import logger

# Set up loguru logger
logger.remove()  # Remove the default logger

# Add custom levels if they don't already exist
if not logger._core.levels.get("SUCCESS"):
    logger.level("SUCCESS", no=25, color="<light-green>", icon="🚀")
if not logger._core.levels.get("DEBUG"):
    logger.level("DEBUG", no=10, color="<light-magenta>", icon="🛰️")
if not logger._core.levels.get("INFO"):
    logger.level("INFO", no=20, color="<light-yellow>", icon="🌌")
if not logger._core.levels.get("WARNING"):
    logger.level("WARNING", no=30, color="<light-red>", icon="🚨")
if not logger._core.levels.get("ERROR"):
    logger.level("ERROR", no=40, color="<red>", icon="❗")
if not logger._core.levels.get("CRITICAL"):
    logger.level("CRITICAL", no=50, color="<bold red>", icon="🔫")

# Add custom logger with color settings for console
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD}</green> <cyan>{time:HH:mm:ss}</cyan> | "
           "<level>{level.icon} {level: <8}</level> | "
           "<light-cyan>{message}</light-cyan>",
    level="DEBUG",
    colorize=True,
    backtrace=True,
    diagnose=True,
)

# Add plain text logger for file
logger.add(
    "buzz_sync.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level.icon} {level: <8} | {message}",
    level="DEBUG",
    colorize=False,
    backtrace=True,
    diagnose=True,
)

# Load environment variables from .env file
load_dotenv()

# Set log level based on environment variable
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logger.remove()  # Remove the default logger
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD}</green> <cyan>{time:HH:mm:ss}</cyan> | "
           "<level>{level.icon} {level: <8}</level> | "
           "<light-cyan>{message}</light-cyan>",
    level=log_level,
    colorize=True,
    backtrace=True,
    diagnose=True,
)
logger.add(
    "buzz_sync.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level.icon} {level: <8} | {message}",
    level=log_level,
    colorize=False,
    backtrace=True,
    diagnose=True,
)

# Function to retry API calls with exponential backoff
async def retry_api_call(func, *args, retries=5, backoff_in_seconds=1, **kwargs):
    for attempt in range(retries):
        try:
            return await func(*args, **kwargs)
        except (aiohttp.ClientError, aiohttp.ClientResponseError) as e:
            logger.error(f"API call failed: {e}. Retrying in {backoff_in_seconds} seconds (attempt {attempt + 1}/{retries})...")
            await asyncio.sleep(backoff_in_seconds)
            backoff_in_seconds = min(backoff_in_seconds * 2, 60)  # Cap backoff time to 60 seconds
    logger.critical(f"API call failed after {retries} retries.")
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
                            logger.debug(f"Fetched {len(selected_files)} selected files for torrent {torrent_id}")
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
    logger.debug(f"Fetching existing torrents for secondary account with API key {api_key[:5]}...")
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

    logger.debug(f"Fetched {len(torrents)} existing torrents for secondary account with API key {api_key[:5]}")
    return torrents

# Function to delete a torrent
async def delete_torrent(session, api_key, torrent_id):
    logger.debug(f"Deleting torrent ID {torrent_id} from secondary account with API key {api_key[:5]}...")
    if dry_run:
        logger.info(f"Dry run enabled. Skipping deletion of torrent ID {torrent_id}.")
        return None

    headers = {"Authorization": f"Bearer {api_key}"}
    url = f"https://api.real-debrid.com/rest/1.0/torrents/delete/{torrent_id}"
    try:
        async with session.delete(url, headers=headers) as response:
            if response.status != 204:
                logger.error(f"Failed to delete torrent ID {torrent_id}: HTTP {response.status}")
                return None
            return torrent_id
    except aiohttp.ClientError as e:
        logger.error(f"Request failed: {e}")
        return None

# Function to add or update a torrent by its hash and select specified files
async def add_or_update_torrent_and_select_files(session, api_key, existing_torrents, torrent_hash, file_ids):
    logger.debug(f"Adding or updating torrent {torrent_hash} in secondary account with API key {api_key[:5]}...")
    if dry_run:
        logger.info(f"Dry run enabled. Skipping adding or updating hash {torrent_hash}.")
        return None

    headers = {"Authorization": f"Bearer {api_key}"}
    magnet_link = f"magnet:?xt=urn:btih:{torrent_hash}"
    torrent_id = None
    try:
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
                        logger.debug(f"Updating selected files for torrent {torrent_hash}...")
                        select_url = f"https://api.real-debrid.com/rest/1.0/torrents/selectFiles/{existing_torrent_id}"
                        async with session.post(select_url, headers=headers, data={"files": ",".join(map(str, file_ids))}) as response:
                            if response.status in [204, 202]:
                                logger.info(f"Selected files updated for torrent {existing_torrent_id}.")
                            else:
                                logger.error(f"Failed to update selected files: HTTP {response.status}")
                    else:
                        logger.info(f"Selected files for torrent {torrent_hash} are already up-to-date.")
                else:
                    logger.error(f"Failed to fetch files for existing torrent {existing_torrent_id}: HTTP {existing_files_response.status}")
        else:
            logger.debug(f"Adding new torrent {torrent_hash}...")

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
                await asyncio.sleep(15)

                # Select files to download
                logger.info(f"Selecting files for new torrent {torrent_id}...")
                select_url = f"https://api.real-debrid.com/rest/1.0/torrents/selectFiles/{torrent_id}"
                async with session.post(select_url, headers=headers, data={"files": ",".join(map(str, file_ids))}) as response:
                    if response.status in [204, 202]:
                        logger.info(f"Selected files updated for torrent {torrent_id}.")
                    else:
                        logger.error(f"Failed to select files: HTTP {response.status}")

        return torrent_id
    except aiohttp.ClientError as e:
        logger.error(f"Request failed: {e}")
        return None

# Function to find differences between two lists
def list_differences(list1, list2):
    set1 = set(list1)
    set2 = set(list2)
    
    only_in_list1 = list(set1 - set2)
    only_in_list2 = list(set2 - set1)
    
    return only_in_list1, only_in_list2

# Function to sync primary account with secondary accounts
async def sync_accounts(primary_api_key, secondary_api_keys):
    logger.debug("Starting account synchronization...")
    async with aiohttp.ClientSession() as session:
        # Get all hashes and selected files from primary account
        logger.info("Fetching all torrent hashes and selected files from primary account...")
        primary_torrents = await get_all_torrent_hashes_and_files(session, primary_api_key)
        primary_hashes = {torrent_hash for torrent_hash, _ in primary_torrents}
        logger.info(f"Primary account has {len(primary_torrents)} torrents.")

        for api_key in secondary_api_keys:
            logger.info(f"Syncing with secondary accounts...")
            secondary_existing_torrents = await fetch_existing_torrents(session, api_key)
            secondary_hashes = {torrent['hash'] for torrent in secondary_existing_torrents}

            # Find differences
            torrents_to_add, torrents_to_delete = list_differences(primary_hashes, secondary_hashes)

            # Create tasks for adding/updating torrents
            update_tasks = [
                add_or_update_torrent_and_select_files(session, api_key, secondary_existing_torrents, torrent_hash, file_ids)
                for torrent_hash, file_ids in primary_torrents
                if torrent_hash in torrents_to_add
            ]

            # Create tasks for deleting torrents
            delete_tasks = [
                delete_torrent(session, api_key, torrent['id'])
                for torrent in secondary_existing_torrents
                if torrent['hash'] in torrents_to_delete
            ]

            # Combine update and delete tasks
            all_tasks = update_tasks + delete_tasks

            # Process tasks concurrently
            results = await asyncio.gather(*all_tasks, return_exceptions=True)
            for result, task in zip(results, all_tasks):
                if isinstance(result, Exception):
                    logger.error(f"Task failed with exception: {result}")
                else:
                    logger.success(f"Task completed successfully.")

            logger.success(f"Finished syncing with secondary account {api_key}.")

# Check for --dry-run flag
dry_run = '--dry-run' in sys.argv
if dry_run:
    logger.info("Dry run enabled. No changes will be made.")

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

# Main execution with graceful exit handling
def main():
    try:
        asyncio.run(sync_accounts(primary_api_key, secondary_api_keys))
    except KeyboardInterrupt:
        logger.warning("Script interrupted by user. Exiting...")

if __name__ == "__main__":
    main()
