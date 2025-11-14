#!/usr/bin/env python3
"""
Reworked Apple lookup processor:
- Uses dictionary cursor
- Batches Apple API calls (comma-separated ids)
- Writes changed rows to `apple_live_derived_table` (upsert)
- Keeps an audit `podcast_changes` table and `podcast_empty_responses`
- Checkpoint stores last processed source id AND apple_id
"""

import sys
import json
import time
import signal
import logging
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional
import mysql.connector
from mysql.connector import Error
import password
# ------------------------
# CONFIG
# ------------------------
DB_CONFIG = {
    'host': password.host,
    'user': password.user,
    'password': password.password,
    'database': password.database,
    'charset': 'utf8mb4'
}

BATCH_SIZE = 10  # number of rows fetched from source table per DB batch
API_ENDPOINT = "https://itunes.apple.com/lookup"
API_TIMEOUT = 30
DELAY_BETWEEN_REQUESTS = 0.5  # small delay to be polite; we batch API calls anyway
MAX_IDS_PER_API_CALL = 10  # apple lookup supports many ids in one call; keep safe limit
API_RETRY_SLEEP = 2
API_MAX_RETRIES = 3

SOURCE_TABLE = "apple_podcasts"
LIVE_DERIVED_TABLE = "apple_live_derived_table"      # writes updated live data here
CHANGES_TABLE = "podcast_changes"                   # audit of detected changes
EMPTY_RESPONSE_TABLE = "podcast_empty_responses"
CHECKPOINT_TABLE = "podcast_checkpoint"

COLUMN_MAPPING = {
    'apple_podcast_url': 'collectionViewUrl',
    'category': 'primaryGenreName',
    'image': 'artworkUrl100',
    'name': 'collectionName',
    'artistName': 'artistName',
    'feed_url': 'feedUrl',
    'apple_id': 'collectionId'
}

COLUMNS_TO_CHECK = ['apple_podcast_url', 'category', 'image', 'name', 'artistName', 'feed_url']

# ------------------------
# LOGGING
# ------------------------
import os
log_dir = "podcast_logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, f"podcast_processor_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ------------------------
# GLOBALS
# ------------------------
shutdown_requested = False

def signal_handler(signum, frame):
    global shutdown_requested
    logger.warning(f"Received shutdown signal ({signum}) — will stop after current batch.")
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ------------------------
# DB helpers
# ------------------------
def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            logger.info("DB connected")
            return conn
    except Error as e:
        logger.exception("DB connection failed")
        raise

def ensure_tables_exist(cursor):
    """
    Creates:
      - CHANGES_TABLE (audit)
      - EMPTY_RESPONSE_TABLE
      - CHECKPOINT_TABLE (stores last_processed_id and last_processed_apple_id)
      - LIVE_DERIVED_TABLE (stores latest apple-derived fields)
    Adjust columns as needed for your schema.
    """
    create_changes = f"""
    CREATE TABLE IF NOT EXISTS `{CHANGES_TABLE}` (
        id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        source_record_id INT UNSIGNED NOT NULL,
        apple_id BIGINT NOT NULL,
        changes_detected JSON NOT NULL,
        detected_at DATETIME NOT NULL,
        INDEX idx_source_id (source_record_id),
        INDEX idx_apple_id (apple_id),
        INDEX idx_detected_at (detected_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    create_empty = f"""
    CREATE TABLE IF NOT EXISTS `{EMPTY_RESPONSE_TABLE}` (
        id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        source_record_id INT UNSIGNED NOT NULL,
        apple_id BIGINT NOT NULL,
        reason VARCHAR(255) DEFAULT NULL,
        recorded_at DATETIME NOT NULL,
        INDEX idx_source_id (source_record_id),
        INDEX idx_apple_id (apple_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    create_checkpoint = f"""
    CREATE TABLE IF NOT EXISTS `{CHECKPOINT_TABLE}` (
        id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        last_processed_id INT UNSIGNED NOT NULL,
        last_processed_apple_id BIGINT NULL,
        last_run_date DATETIME NOT NULL,
        INDEX idx_last_run (last_run_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    # Simple LIVE_DERIVED_TABLE schema — adjust types/columns to match your source
    create_live = f"""
    CREATE TABLE IF NOT EXISTS `{LIVE_DERIVED_TABLE}` (
        apple_id BIGINT NOT NULL PRIMARY KEY,
        apple_podcast_url VARCHAR(1024),
        category VARCHAR(255),
        image VARCHAR(1024),
        name VARCHAR(1024),
        artistName VARCHAR(512),
        feed_url VARCHAR(1024),
        last_updated DATETIME,
        INDEX idx_category (category)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    for q in (create_changes, create_empty, create_checkpoint, create_live):
        cursor.execute(q)
    logger.info("Ensured required tables exist")

def get_last_checkpoint(cursor) -> Dict[str, Optional[int]]:
    """Return dict {'last_processed_id': int, 'last_processed_apple_id': int|None}"""
    q = f"SELECT last_processed_id, last_processed_apple_id FROM `{CHECKPOINT_TABLE}` ORDER BY id DESC LIMIT 1"
    cursor.execute(q)
    row = cursor.fetchone()
    if row:
        # cursor is dictionary cursor; values accessible by key or index
        last_id = row.get('last_processed_id') if isinstance(row, dict) else row[0]
        last_apple = row.get('last_processed_apple_id') if isinstance(row, dict) else row[1]
        logger.info(f"Found checkpoint: id={last_id}, apple_id={last_apple}")
        return {'last_processed_id': int(last_id), 'last_processed_apple_id': int(last_apple) if last_apple else None}
    logger.info("No checkpoint found, starting fresh")
    return {'last_processed_id': 0, 'last_processed_apple_id': None}

def save_checkpoint(cursor, connection, last_id: int, last_apple_id: Optional[int]):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    q = f"INSERT INTO `{CHECKPOINT_TABLE}` (last_processed_id, last_processed_apple_id, last_run_date) VALUES (%s, %s, %s)"
    cursor.execute(q, (last_id, last_apple_id, now))
    connection.commit()
    logger.info(f"Saved checkpoint id={last_id} apple_id={last_apple_id}")

# ------------------------
# Fetch source rows
# ------------------------
def fetch_batch_records(cursor, last_id: int, batch_size: int, target_date: str) -> List[Dict[str,Any]]:
    """
    Fetch rows with id > last_id, limited by batch_size. Adjust date column name if required.
    Assumes the source table has: id, apple_id, apple_podcast_url, category, image, name, artistName, feed_url, created_date
    If your table doesn't have created_date, remove that filter or replace it.
    """
    cols = ['id'] + COLUMNS_TO_CHECK + ['apple_id']
    columns_str = ', '.join(cols)
    query = f"""
    SELECT {columns_str}
    FROM `{SOURCE_TABLE}`
    WHERE id > %s
      AND apple_id IS NOT NULL
      AND DATE(updated_at) > ?
    ORDER BY id ASC
    LIMIT %s
    """
    cursor.execute(query, (last_id, target_date, batch_size))
    rows = cursor.fetchall() or []
    logger.info(f"Fetched {len(rows)} records from source (id > {last_id})")
    return rows

# ------------------------
# Apple API (batched)
# ------------------------
def call_apple_lookup_api_batch(apple_ids: List[int]) -> Dict[int, Dict[str,Any]]:
    """
    Calls lookup API with comma-separated ids and returns mapping apple_id -> api result dict.
    If an apple_id has no result, it will not be present in the returned dict.
    """
    results_map: Dict[int, Dict[str,Any]] = {}
    if not apple_ids:
        return results_map

    # chunk the ids into safe-sized groups
    for i in range(0, len(apple_ids), MAX_IDS_PER_API_CALL):
        chunk = apple_ids[i:i+MAX_IDS_PER_API_CALL]
        params = {'id': ','.join(str(x) for x in chunk), 'entity': 'podcast'}
        attempt = 0
        while attempt < API_MAX_RETRIES:
            try:
                r = requests.get(API_ENDPOINT, params=params, timeout=API_TIMEOUT)
                if r.status_code != 200:
                    logger.warning(f"Apple API returned {r.status_code} for ids chunk starting {chunk[0]}")
                    attempt += 1
                    time.sleep(API_RETRY_SLEEP * attempt)
                    continue
                data = r.json()
                if 'results' in data:
                    for item in data['results']:
                        # collectionId is the apple_id for podcasts
                        cid = item.get('collectionId') or item.get('collectionId')  # defensive
                        if cid:
                            results_map[int(cid)] = item
                break
            except requests.exceptions.RequestException as e:
                attempt += 1
                logger.warning(f"API request error (attempt {attempt}) for chunk: {e}")
                time.sleep(API_RETRY_SLEEP * attempt)
            except ValueError as e:
                # JSON decode error
                logger.error(f"JSON decode error from Apple API: {e}")
                break
        # small delay between chunked calls
        time.sleep(DELAY_BETWEEN_REQUESTS)
    return results_map

# ------------------------
# Mapping & change detection
# ------------------------
def get_artwork_url(api_data: Dict) -> Optional[str]:
    for f in ('artworkUrl100', 'artworkUrl60', 'artworkUrl30'):
        url = api_data.get(f)
        if url:
            return url
    return None

def normalize_image_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    if '.jpg' in url:
        return url.split('.jpg')[0] + '.jpg'
    return url

def map_api_to_db_columns(api_data: Dict) -> Dict[str,Any]:
    mapped = {}
    for db_col, api_field in COLUMN_MAPPING.items():
        if db_col == 'image':
            mapped[db_col] = get_artwork_url(api_data)
        else:
            mapped[db_col] = api_data.get(api_field)
    return mapped

def detect_changes(old_record: Dict[str,Any], api_data: Dict[str,Any], columns: List[str]) -> Dict[str, Dict[str,Any]]:
    changes = {}
    new_data = map_api_to_db_columns(api_data)
    for col in columns:
        old_val = old_record.get(col)
        new_val = new_data.get(col)
        if col == 'image':
            old_norm = normalize_image_url(old_val)
            new_norm = normalize_image_url(new_val)
            if old_norm != new_norm:
                changes[col] = {'old': old_val, 'new': new_val, 'old_normalized': old_norm, 'new_normalized': new_norm}
        else:
            old_s = str(old_val) if old_val is not None else None
            new_s = str(new_val) if new_val is not None else None
            if old_s != new_s:
                changes[col] = {'old': old_val, 'new': new_val}
    return changes

# ------------------------
# Persistence helpers
# ------------------------
def save_changes_to_table_b(cursor, connection, record_id: int, apple_id: int, changes: Dict):
    q = f"INSERT INTO `{CHANGES_TABLE}` (source_record_id, apple_id, changes_detected, detected_at) VALUES (%s, %s, %s, %s)"
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute(q, (record_id, apple_id, json.dumps(changes, ensure_ascii=False), now))
    connection.commit()
    logger.info(f"Saved {len(changes)} changes to {CHANGES_TABLE} for source id {record_id}")

def save_empty_response_to_table_d(cursor, connection, record_id: int, apple_id: int, reason: str = 'No results from Apple API'):
    q = f"INSERT INTO `{EMPTY_RESPONSE_TABLE}` (source_record_id, apple_id, reason, recorded_at) VALUES (%s, %s, %s, %s)"
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute(q, (record_id, apple_id, reason, now))
    connection.commit()
    logger.info(f"Saved empty response for source id {record_id}, apple_id {apple_id}")

def upsert_into_live_table(cursor, connection, apple_id: int, mapped_data: Dict[str,Any]):
    """
    Upsert into LIVE_DERIVED_TABLE using apple_id as primary key.
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # ensure keys match columns: apple_podcast_url, category, image, name, artistName, feed_url
    q = f"""
    INSERT INTO `{LIVE_DERIVED_TABLE}` (apple_id, apple_podcast_url, category, image, name, artistName, feed_url, last_updated)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      apple_podcast_url = VALUES(apple_podcast_url),
      category = VALUES(category),
      image = VALUES(image),
      name = VALUES(name),
      artistName = VALUES(artistName),
      feed_url = VALUES(feed_url),
      last_updated = VALUES(last_updated)
    """
    cursor.execute(q, (
        apple_id,
        mapped_data.get('apple_podcast_url'),
        mapped_data.get('category'),
        mapped_data.get('image'),
        mapped_data.get('name'),
        mapped_data.get('artistName'),
        mapped_data.get('feed_url'),
        now
    ))
    connection.commit()
    logger.info(f"Upserted apple_id {apple_id} into {LIVE_DERIVED_TABLE}")

# ------------------------
# Main processing
# ------------------------
def process_batch(connection, target_date: str):
    global shutdown_requested
    cursor = connection.cursor(dictionary=True)
    ensure_tables_exist(cursor)

    checkpoint = get_last_checkpoint(cursor)
    last_processed_id = checkpoint['last_processed_id']
    last_processed_apple_id = checkpoint.get('last_processed_apple_id')

    totals = {'processed': 0, 'changes': 0, 'empty': 0, 'errors': 0, 'no_change': 0}

    try:
        while not shutdown_requested:
            rows = fetch_batch_records(cursor, last_processed_id, BATCH_SIZE, target_date)
            if not rows:
                logger.info("No more rows to process")
                break

            # collect apple_ids and call API once per chunk
            apple_ids = [int(r['apple_id']) for r in rows if r.get('apple_id')]
            api_map = call_apple_lookup_api_batch(apple_ids)

            for row in rows:
                if shutdown_requested:
                    break
                rid = int(row['id'])
                aid = int(row['apple_id'])
                last_processed_id = rid
                last_processed_apple_id = aid
                totals['processed'] += 1

                api_data = api_map.get(aid)
                if not api_data:
                    # no result for this apple id
                    save_empty_response_to_table_d(cursor, connection, rid, aid, reason='No results')
                    totals['empty'] += 1
                    continue

                changes = detect_changes(row, api_data, COLUMNS_TO_CHECK)
                mapped = map_api_to_db_columns(api_data)
                if changes:
                    save_changes_to_table_b(cursor, connection, rid, aid, changes)
                    totals['changes'] += 1
                    # write mapped data to live derived table
                    upsert_into_live_table(cursor, connection, aid, mapped)
                else:
                    totals['no_change'] += 1
                    # Even if no changes, it's often good to refresh last_updated
                    upsert_into_live_table(cursor, connection, aid, mapped)

            # after processing this DB batch, save checkpoint
            save_checkpoint(cursor, connection, last_processed_id, last_processed_apple_id)
            logger.info(f"Batch complete. Totals so far: {totals}")
            if shutdown_requested:
                break

        # final checkpoint save
        save_checkpoint(cursor, connection, last_processed_id, last_processed_apple_id)
        logger.info("Processing finished")
        logger.info(f"Totals: {totals}")
    except Exception as e:
        logger.exception("Fatal error during processing")
        # save checkpoint on error
        try:
            save_checkpoint(cursor, connection, last_processed_id, last_processed_apple_id)
        except Exception:
            logger.exception("Failed to save checkpoint after error")
    finally:
        cursor.close()

def main():
    target_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
    conn = get_db_connection()
    try:
        process_batch(conn, target_date)
    finally:
        if conn.is_connected():
            conn.close()
            logger.info("DB connection closed")

if __name__ == "__main__":
    main()
