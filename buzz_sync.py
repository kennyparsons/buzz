import os
from dotenv import load_dotenv
from rdapi import RD
from requests.exceptions import RequestException, HTTPError
import time
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Custom logging formatter with timestamp, log level emoji, and message
class CustomFormatter(logging.Formatter):
    format = "%(asctime)s | %(levelname_emoji)s %(levelname)-8s | %(message)s"
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
        return super().format(record)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(CustomFormatter())
logger.handlers = [handler]

# Function to retry API calls with exponential backoff
def retry_api_call(func, *args, retries=5, backoff_in_seconds=1, **kwargs):
    for i in range(retries):
        try:
            return func(*args, **kwargs)
        except (RequestException, HTTPError) as e:
            logging.error(f"API call failed: {e}. Retrying in {backoff_in_seconds} seconds...")
            time.sleep(backoff_in_seconds)
            backoff_in_seconds *= 2
    return None

# Function to get torrent hashes and selected files for a specific page
def get_torrent_hashes_and_files_page(rd_instance, page):
    logging.info(f"Fetching torrent hashes and files for page {page}")
    try:
        response = retry_api_call(rd_instance.torrents.get, limit=50, page=page)
        if response is None or response.status_code != 200:
            logging.error(f"Failed to fetch torrents: HTTP {response.status_code if response else 'None'} for page {page}")
            return []

        response_json = response.json()
        if isinstance(response_json, dict) and 'error' in response_json:
            logging.error(f"Error fetching torrents: {response_json['error']} for page {page}")
            return []

        torrents = []
        for torrent in response_json:
            if isinstance(torrent, dict):
                torrent_id = torrent['id']
                files_response = retry_api_call(rd_instance.torrents.info, torrent_id)
                if files_response and files_response.status_code == 200:
                    files_info = files_response.json()
                    selected_files = [file['id'] for file in files_info.get('files', []) if file['selected']]
                    torrents.append((torrent['hash'], selected_files))
                    logging.info(f"Fetched {len(selected_files)} selected files for torrent {torrent_id}")
                else:
                    logging.error(f"Failed to fetch files for torrent {torrent_id}: HTTP {files_response.status_code if files_response else 'None'}")
        return torrents
    except RequestException as e:
        logging.error(f"Request failed: {e}. Page: {page}")
        return []
    except ValueError as e:
        logging.error(f"Failed to decode JSON response: {e}. Page: {page} Raw response: {response.text}")
        return []

# Function to get all torrent hashes and selected files concurrently
def get_all_torrent_hashes_and_files(rd_instance, max_workers):
    torrents = []
    page = 1

    # First, determine the number of pages
    try:
        response = retry_api_call(rd_instance.torrents.get, limit=50, page=page)
        if response is None or response.status_code != 200:
            logging.error(f"Initial request failed: HTTP {response.status_code if response else 'None'}")
            return []
        total_torrents = int(response.headers.get('X-Total-Count', 0))
        total_pages = (total_torrents // 50) + 1
        logging.info(f"Total torrents: {total_torrents}, Total pages: {total_pages}")
    except (RequestException, ValueError) as e:
        logging.error(f"Initial request failed: {e}")
        return []

    # Use ThreadPoolExecutor to fetch pages concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {executor.submit(get_torrent_hashes_and_files_page, rd_instance, page): page for page in range(1, total_pages + 1)}

        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                page_torrents = future.result()
                if page_torrents:
                    torrents.extend(page_torrents)
                    logging.info(f"Completed fetching for page {page}")
            except Exception as e:
                logging.error(f"Error fetching page {page}: {e}")

    return torrents

# Function to fetch all existing torrents for a secondary account
def fetch_existing_torrents(rd_instance):
    torrents = []
    page = 1

    while True:
        response = retry_api_call(rd_instance.torrents.get, limit=50, page=page)
        if response is None:
            logging.error(f"Failed to fetch existing torrents for page {page}")
            return []
        if response.status_code == 204:  # No Content
            break

        response_json = response.json()
        if isinstance(response_json, list) and not response_json:
            break

        torrents.extend(response_json)
        page += 1

        if len(response_json) < 50:
            break

    return torrents

# Function to add or update a torrent by its hash and select specified files
def add_or_update_torrent_and_select_files(rd_instance, torrent_hash, file_ids):
    if dry_run:
        logging.info(f"Dry run enabled. Skipping adding or updating hash {torrent_hash}.")
        return None

    try:
        # Check if the torrent already exists
        existing_torrents = fetch_existing_torrents(rd_instance)
        existing_torrent_ids = [t['hash'] for t in existing_torrents]
        if torrent_hash in existing_torrent_ids:
            logging.info(f"Torrent {torrent_hash} already exists. Checking selected files...")

            # Fetch existing torrent info
            existing_torrent = next(t for t in existing_torrents if t['hash'] == torrent_hash)
            existing_torrent_id = existing_torrent['id']
            existing_files_response = retry_api_call(rd_instance.torrents.info, existing_torrent_id)

            if existing_files_response and existing_files_response.status_code == 200:
                existing_files_info = existing_files_response.json()
                existing_selected_files = [file['id'] for file in existing_files_info.get('files', []) if file['selected']]

                # If the selected files differ, update them
                if set(existing_selected_files) != set(file_ids):
                    logging.info(f"Updating selected files for torrent {torrent_hash}...")
                    response = retry_api_call(rd_instance.torrents.select_files, existing_torrent_id, ",".join(map(str, file_ids)))
                    if response is None or response.status_code not in [204, 200]:
                        logging.error(f"Failed to update selected files: HTTP {response.status_code if response else 'None'}")
                        return None
                else:
                    logging.info(f"Selected files for torrent {torrent_hash} are already up-to-date.")
            else:
                logging.error(f"Failed to fetch files for existing torrent {existing_torrent_id}: HTTP {existing_files_response.status_code if existing_files_response else 'None'}")
        else:
            logging.info(f"Adding new torrent {torrent_hash}...")

            # Add the torrent
            response = retry_api_call(rd_instance.torrents.add_magnet, magnet=torrent_hash)
            if response is None or response.status_code != 201:
                logging.error(f"Failed to add torrent: HTTP {response.status_code if response else 'None'}")
                return None

            torrent_id = response.json().get('id')
            if not torrent_id:
                logging.error(f"Failed to get torrent ID after adding torrent: {response.json()}")
                return None

            # Select files to download
            response = retry_api_call(rd_instance.torrents.select_files, torrent_id, ",".join(map(str, file_ids)))
            if response is None or response.status_code not in [204, 200]:
                logging.error(f"Failed to select files: HTTP {response.status_code if response else 'None'}")
                return None

        return torrent_id
    except RequestException as e:
        logging.error(f"Request failed: {e}")
        return None
    except ValueError as e:
        logging.error(f"Failed to decode JSON response: {e}. Raw response: {response.text}")
        return None

# Function to sync primary account with secondary accounts
def sync_accounts(primary_rd, secondary_rds, max_workers):
    # Get all hashes and selected files from primary account
    primary_torrents = get_all_torrent_hashes_and_files(primary_rd, max_workers)
    logging.info(f"Primary account has {len(primary_torrents)} torrents.")

    for rd in secondary_rds:
        logging.info(f"Syncing with secondary account...")
        secondary_hashes = {torrent[0] for torrent in get_all_torrent_hashes_and_files(rd, max_workers)}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_torrent = {
                executor.submit(add_or_update_torrent_and_select_files, rd, torrent_hash, file_ids): (torrent_hash, file_ids)
                for torrent_hash, file_ids in primary_torrents if torrent_hash not in secondary_hashes
            }

            for future in as_completed(future_to_torrent):
                torrent_hash, file_ids = future_to_torrent[future]
                try:
                    result = future.result()
                    if result:
                        logging.info(f"Added or updated hash {torrent_hash} successfully and selected files.")
                    else:
                        logging.error(f"Failed to add or update hash {torrent_hash}.")
                except Exception as e:
                    logging.error(f"Error processing torrent {torrent_hash}: {e}")

# Check for --dry-run flag
dry_run = '--dry-run' in sys.argv
if dry_run:
    logging.info("Dry run enabled. No changes will be made.")

# Load environment variables from .env file
if not os.path.exists('.env'):
    logging.error("Please create a .env file with the following contents:")
    logging.error("RD_PRIMARY_API_KEY=your_primary_api_key")
    logging.error("RD_SECONDARY_API_KEYS=your_secondary_api_key1,your_secondary_api_key2")
    sys.exit(1)

load_dotenv()

# Load primary and secondary API keys from environment variables
primary_api_key = os.getenv('RD_PRIMARY_API_KEY')
secondary_api_keys = os.getenv('RD_SECONDARY_API_KEYS')

# Validate environment variables
if primary_api_key is None:
    logging.error("RD_PRIMARY_API_KEY is not set in the .env file.")
    sys.exit(1)

if secondary_api_keys is None:
    logging.error("RD_SECONDARY_API_KEYS is not set in the .env file.")
    sys.exit(1)

# Split secondary API keys
secondary_api_keys = secondary_api_keys.split(',')

# Initialize RD instance for primary account
os.environ['RD_APITOKEN'] = primary_api_key
primary_rd = RD()

# Initialize RD instances for secondary accounts
secondary_rds = []
for key in secondary_api_keys:
    os.environ['RD_APITOKEN'] = key
    secondary_rds.append(RD())

# Here be dragons
sync_accounts(primary_rd, secondary_rds, max_workers=20)  # Adjust max_workers as needed
