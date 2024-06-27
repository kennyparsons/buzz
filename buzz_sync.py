import os
from dotenv import load_dotenv
from rdapi import RD
from requests.exceptions import RequestException
import time
import sys
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

# Function to get all torrent hashes
def get_all_torrent_hashes(rd_instance):
    page = 1
    hashes = []

    while True:
        try:
            response = rd_instance.torrents.get(limit=50, page=page).json()
            if isinstance(response, dict) and 'error' in response:
                logging.error(f"Error fetching torrents: {response['error']}")
                return []
        except RequestException as e:
            logging.error(f"Request failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)
            continue
        except ValueError as e:
            logging.error(f"Failed to decode JSON response: {e}. Retrying in 5 seconds...")
            time.sleep(5)
            continue

        if not response:
            break

        for torrent in response:
            if isinstance(torrent, dict):
                hashes.append(torrent['hash'])
            else:
                logging.error(f"Unexpected response format: {torrent}")

        if len(response) < 50:
            break

        page += 1

    return hashes

# Function to add a torrent by its hash
def add_torrent_by_hash(rd_instance, torrent_hash):
    if dry_run:
        logging.info(f"Dry run enabled. Skipping adding hash {torrent_hash}.")
        return None

    try:
        response = rd_instance.torrents.add_magnet(magnet=torrent_hash).json()
        if 'error' in response:
            logging.error(f"Error adding torrent: {response['error']}")
        return response
    except RequestException as e:
        logging.error(f"Request failed: {e}")
        return None
    except ValueError as e:
        logging.error(f"Failed to decode JSON response: {e}")
        return None

# Function to sync primary account with secondary accounts
def sync_accounts(primary_rd, secondary_rds):
    # Get all hashes from primary account
    primary_hashes = get_all_torrent_hashes(primary_rd)
    logging.info(f"Primary account has {len(primary_hashes)} hashes.")

    for rd in secondary_rds:
        logging.info(f"Syncing with secondary account...")
        secondary_hashes = get_all_torrent_hashes(rd)

        for hash in primary_hashes:
            if hash not in secondary_hashes:
                logging.info(f"Adding hash {hash} to secondary account...")
                result = add_torrent_by_hash(rd, hash)
                if not dry_run:
                    if result:
                        logging.info(f"Added hash {hash} successfully.")
                    else:
                        logging.error(f"Failed to add hash {hash}.")


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
sync_accounts(primary_rd, secondary_rds)