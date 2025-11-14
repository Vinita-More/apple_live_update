#!/usr/bin/env python3
# Batch processing script for Apple Podcast data comparison
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

# ==========================================
# CONFIGURATION
# ==========================================

# Database configuration
DB_CONFIG = {
    'host': password.host,
    'user': password.user,
    'password': password.password,
    'database': password.database,
    'charset': 'utf8mb4'
}

# Processing configuration
BATCH_SIZE = 10  # Number of records to process before exit
API_ENDPOINT = "https://itunes.apple.com/lookup"
API_TIMEOUT = 30
DELAY_BETWEEN_REQUESTS = 0.5

# Table names
SOURCE_TABLE = "apple_podcast_live_updates"
CHANGES_TABLE = "apple_ids_derived_table"  # Stores updated values
EMPTY_RESPONSE_TABLE = "apple_live_table_skipped"
CHECKPOINT_TABLE = "apple_ids_derived_index"

# Column mapping: Source DB column -> Changes table column -> API response field
COLUMN_MAPPING = {
    'apple_podcast_url': {'changes_col': 'applePodcastUrl', 'api_field': 'collectionViewUrl'},
    'category': {'changes_col': 'podcastCategory', 'api_field': 'primaryGenreName'},
    'image': {'changes_col': 'imageUrl', 'api_field': 'artworkUrl100'},
    'name': {'changes_col': 'podcastName', 'api_field': 'collectionName'},
    'artistName': {'changes_col': 'artistName', 'api_field': 'artistName'},
    'feed_url': {'changes_col': 'feedUrl', 'api_field': 'feedUrl'}
}

# Columns to check for changes (source table column names)
COLUMNS_TO_CHECK = ['apple_podcast_url', 'category', 'image', 'name', 'artistName', 'feed_url']

# ==========================================
# LOGGING SETUP
# ==========================================

import os
log_dir = "podcast_logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

log_filename = os.path.join(log_dir, "podcast_processor_{date}.log".format(date=datetime.now().strftime('%Y%m%d')))

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

# ==========================================
# GLOBAL VARIABLES
# ==========================================

shutdown_requested = False

# ==========================================
# SIGNAL HANDLING
# ==========================================

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    logger.warning("Received shutdown signal ({}). Will stop after current batch.".format(signum))
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ==========================================
# DATABASE FUNCTIONS
# ==========================================

def get_db_connection():
    """Create and return a database connection"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            logger.info("Database connected successfully")
            return connection
    except Error as e:
        logger.error("Database connection failed: {}".format(e))
        sys.exit(1)

def ensure_tables_exist(cursor):
    """Create required tables if they don't exist"""
    
    # Changes table - stores updated values with apple_id as primary key
    create_changes_table = (
        "CREATE TABLE IF NOT EXISTS `{changes_table}` ("
        "    appleId VARCHAR(255) PRIMARY KEY NOT NULL,"
        "    applePodcastUrl VARCHAR(255),"
        "    podcastCategory VARCHAR(255),"
        "    imageUrl VARCHAR(255),"
        "    podcastName VARCHAR(255),"
        "    artistName VARCHAR(255),"
        "    feedUrl VARCHAR(330),"
        "    updateDate DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
    ).format(changes_table=CHANGES_TABLE)
    
    # Skipped/Empty responses table
    create_empty_table = (
        "CREATE TABLE IF NOT EXISTS `{empty_table}` ("
        "    appleId VARCHAR(255) PRIMARY KEY NOT NULL,"
        "    retryCount INT"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
    ).format(empty_table=EMPTY_RESPONSE_TABLE)
    
    # Checkpoint table
    create_checkpoint_table = (
        "CREATE TABLE IF NOT EXISTS `{checkpoint_table}` ("
        "    appleId VARCHAR(255) PRIMARY KEY NOT NULL,"
        "    updateTime DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
    ).format(checkpoint_table=CHECKPOINT_TABLE)
    
    try:
        cursor.execute(create_changes_table)
        cursor.execute(create_empty_table)
        cursor.execute(create_checkpoint_table)
        logger.info("All required tables verified/created successfully")
        return True
    except Error as e:
        logger.error("Failed to create tables: {}".format(e))
        return False

def get_last_checkpoint(cursor) -> int:
    """Retrieve the last processed apple_id from checkpoint table"""
    try:
        query = (
            "SELECT appleId, updateTime "
            "FROM `{checkpoint_table}` "
            "ORDER BY updateTime DESC "
            "LIMIT 1"
        ).format(checkpoint_table=CHECKPOINT_TABLE)
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result:
            last_apple_id, last_run = result
            logger.info("Found checkpoint: Last Apple ID = {0}, Last Run = {1}".format(last_apple_id, last_run))
            # Convert apple_id to record id for query (assuming apple_id is stored as string)
            # We'll need to query source table to find the record id
            try:
                return int(last_apple_id) if last_apple_id else 0
            except Exception:
                # If appleId isn't numeric, return 0 for safety
                return 0
        else:
            logger.info("No checkpoint found, starting from beginning")
            return 0
    except Error as e:
        logger.error("Error fetching checkpoint: {}".format(e))
        return 0

def save_checkpoint(cursor, connection, apple_id: int) -> bool:
    """Save the current processing checkpoint using UPSERT"""
    try:
        query = (
            "INSERT INTO `{checkpoint_table}` (appleId) "
            "VALUES (%s) "
            "ON DUPLICATE KEY UPDATE "
            "updateTime = CURRENT_TIMESTAMP"
        ).format(checkpoint_table=CHECKPOINT_TABLE)
        cursor.execute(query, (str(apple_id),))
        connection.commit()
        logger.info("Checkpoint saved: Apple ID = {0}".format(apple_id))
        return True
    except Error as e:
        logger.error("Failed to save checkpoint: {}".format(e))
        return False

def fetch_batch_records(cursor, last_apple_id: int, batch_size: int, target_date: str) -> List[Dict]:
    """Fetch a batch of records from the source table"""
    try:
        columns = ', '.join(['id'] + COLUMNS_TO_CHECK + ['apple_id'])
        
        query = (
            "SELECT {columns} "
            "FROM `{source_table}` "
            "WHERE apple_id > %s "
            "AND DATE(updated_at) > %s "
            "AND apple_id IS NOT NULL "
            "ORDER BY apple_id ASC "
            "LIMIT %s"
        ).format(columns=columns, source_table=SOURCE_TABLE)
        
        cursor.execute(query, (last_apple_id, target_date, batch_size))
        
        columns_list = [desc[0] for desc in cursor.description]
        records = []
        
        for row in cursor.fetchall():
            record = dict(zip(columns_list, row))
            records.append(record)
        
        logger.info("Fetched {count} records starting from Apple ID {aid}".format(count=len(records), aid=last_apple_id))
        return records
        
    except Error as e:
        logger.error("Failed to fetch batch records: {}".format(e))
        return []

# ==========================================
# API FUNCTIONS - BATCH PROCESSING
# ==========================================

def call_apple_lookup_api_batch(apple_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """
    Call Apple iTunes Lookup API for multiple apple_ids at once.
    Apple API supports comma-separated IDs.
    Returns a dictionary mapping apple_id -> result
    """
    if not apple_ids:
        return {}
    
    try:
        # Join all apple_ids with commas
        ids_param = ','.join(map(str, apple_ids))
        
        params = {
            'id': ids_param,
            'entity': 'podcast'
        }
        
        logger.info("Calling API with {n} apple_ids: {ids}".format(n=len(apple_ids), ids=apple_ids))
        
        response = requests.get(
            API_ENDPOINT,
            params=params,
            timeout=API_TIMEOUT
        )
        
        if response.status_code != 200:
            logger.warning("API returned HTTP {code}".format(code=response.status_code))
            # Return error for all IDs
            return {
                apple_id: {
                    'success': False, 
                    'error': 'HTTP {code}'.format(code=response.status_code),
                    'http_code': response.status_code
                } 
                for apple_id in apple_ids
            }
        
        data = response.json()
        
        # Process results
        results_map = {}
        
        if 'results' in data and len(data['results']) > 0:
            # Map each result to its apple_id
            for result in data['results']:
                result_apple_id = result.get('collectionId')
                if result_apple_id:
                    results_map[result_apple_id] = {
                        'success': True,
                        'data': result,
                        'empty': False
                    }
            
            logger.info("API returned {count} results".format(count=len(results_map)))
        
        # Mark any IDs that didn't return results as empty
        for apple_id in apple_ids:
            if apple_id not in results_map:
                results_map[apple_id] = {
                    'success': True,
                    'empty': True
                }
                logger.warning("No result returned for Apple ID {id}".format(id=apple_id))
        
        return results_map
        
    except requests.exceptions.Timeout:
        logger.error("API timeout for batch request")
        return {apple_id: {'success': False, 'error': 'Timeout'} for apple_id in apple_ids}
    
    except requests.exceptions.RequestException as e:
        logger.error("API call failed: {err}".format(err=e))
        return {apple_id: {'success': False, 'error': str(e)} for apple_id in apple_ids}
    
    except json.JSONDecodeError:
        logger.error("Invalid JSON response")
        return {apple_id: {'success': False, 'error': 'Invalid JSON'} for apple_id in apple_ids}

# ==========================================
# CHANGE DETECTION FUNCTIONS
# ==========================================

def get_artwork_url(api_data: Dict) -> Optional[str]:
    """Get artwork URL from API response with fallback logic"""
    artwork_fields = ['artworkUrl100', 'artworkUrl60', 'artworkUrl30']
    
    for field in artwork_fields:
        url = api_data.get(field)
        if url:
            return url
    
    return None

def normalize_image_url(url: Optional[str]) -> Optional[str]:
    """Normalize image URL by removing size suffix"""
    if not url:
        return None
    
    if '.jpg' in url:
        base_url = url.split('.jpg')[0] + '.jpg'
        return base_url
    
    return url

def map_api_to_db_columns(api_data: Dict) -> Dict:
    """Map API response fields to changes table column names and values"""
    mapped_data = {}
    
    for source_col, mapping in COLUMN_MAPPING.items():
        changes_col = mapping['changes_col']
        api_field = mapping['api_field']
        
        if source_col == 'image':
            # Special handling for image field
            mapped_data[changes_col] = get_artwork_url(api_data)
        else:
            mapped_data[changes_col] = api_data.get(api_field)
    
    return mapped_data

def detect_changes(old_record: Dict, api_data: Dict, columns: List[str]) -> tuple:
    """
    Detect changes between database record and API response.
    Returns: (has_changes: bool, new_values_for_changes_table: Dict)
    """
    has_changes = False
    new_values = {}
    
    # Get API data mapped to changes table columns
    api_mapped = map_api_to_db_columns(api_data)
    
    for source_col in columns:
        old_value = old_record.get(source_col)
        
        # Get the changes table column name for this source column
        changes_col = COLUMN_MAPPING[source_col]['changes_col']
        new_value = api_mapped.get(changes_col)
        
        # Store new value for changes table
        new_values[changes_col] = new_value
        
        # Check if value changed
        if source_col == 'image':
            old_normalized = normalize_image_url(old_value)
            new_normalized = normalize_image_url(new_value)
            
            if old_normalized != new_normalized:
                has_changes = True
                logger.debug("Change detected in {col}: {old} -> {new}".format(col=source_col, old=old_value, new=new_value))
        else:
            old_str = str(old_value) if old_value is not None else None
            new_str = str(new_value) if new_value is not None else None
            
            if old_str != new_str:
                has_changes = True
                logger.debug("Change detected in {col}: {old} -> {new}".format(col=source_col, old=old_value, new=new_value))
    
    return has_changes, new_values

def save_changes_to_table(cursor, connection, apple_id: int, new_values: Dict) -> bool:
    """
    Save updated values to changes table using UPSERT (INSERT ... ON DUPLICATE KEY UPDATE).
    This will insert new records or update existing ones based on appleId primary key.
    """
    try:
        # Build column names and placeholders
        columns = ['appleId'] + list(new_values.keys())
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Build UPDATE clause for ON DUPLICATE KEY
        update_clause = ', '.join(["`{col}` = VALUES(`{col}`)".format(col=col) for col in new_values.keys()])
        
        query = (
            "INSERT INTO `{changes_table}` "
            "(`{cols}`) "
            "VALUES ({placeholders}) "
            "ON DUPLICATE KEY UPDATE "
            "{update_clause}, "
            "updateDate = CURRENT_TIMESTAMP"
        ).format(changes_table=CHANGES_TABLE, cols="`, `".join(columns), placeholders=placeholders, update_clause=update_clause)
        
        # Prepare values: appleId + all new values
        values = [str(apple_id)] + list(new_values.values())
        
        cursor.execute(query, values)
        connection.commit()
        
        logger.info("Updated changes table for Apple ID {id}: {count} columns".format(id=apple_id, count=len(new_values)))
        return True
        
    except Error as e:
        logger.error("Failed to save changes for Apple ID {id}: {err}".format(id=apple_id, err=e))
        return False

def save_empty_response_to_table(cursor, connection, apple_id: int) -> bool:
    """
    Save/update empty response record to skipped table using UPSERT.
    Increments retryCount each time.
    """
    try:
        query = (
            "INSERT INTO `{empty_table}` "
            "(appleId, retryCount) "
            "VALUES (%s, 1) "
            "ON DUPLICATE KEY UPDATE "
            "retryCount = retryCount + 1"
        ).format(empty_table=EMPTY_RESPONSE_TABLE)
        
        cursor.execute(query, (str(apple_id),))
        connection.commit()
        
        # Get the current retry count
        cursor.execute("SELECT retryCount FROM `{empty_table}` WHERE appleId = %s".format(empty_table=EMPTY_RESPONSE_TABLE), (str(apple_id),))
        result = cursor.fetchone()
        retry_count = result[0] if result else 1
        
        logger.info("Empty response recorded for Apple ID {id} (retry count: {count})".format(id=apple_id, count=retry_count))
        return True
        
    except Error as e:
        logger.error("Failed to save empty response for Apple ID {id}: {err}".format(id=apple_id, err=e))
        return False

# ==========================================
# MAIN PROCESSING FUNCTION
# ==========================================

def process_batch(connection, target_date: str) -> bool:
    """Main batch processing function - processes exactly BATCH_SIZE records then exits"""
    global shutdown_requested
    
    logger.info("=== STARTING APPLE PODCAST BATCH PROCESSING ===")
    logger.info("Target Date: {date}".format(date=target_date))
    logger.info("Batch Size: {size}".format(size=BATCH_SIZE))
    
    cursor = connection.cursor()
    
    # Ensure tables exist
    if not ensure_tables_exist(cursor):
        logger.error("Failed to verify/create required tables")
        return False
    
    # Get last checkpoint (apple_id)
    last_processed_apple_id = get_last_checkpoint(cursor)
    
    total_changes = 0
    total_empty = 0
    total_errors = 0
    total_no_change = 0
    
    try:
        # Step 1: Fetch records from source table (hold in memory)
        records = fetch_batch_records(cursor, last_processed_apple_id, BATCH_SIZE, target_date)
        
        if not records:
            logger.info("No records to process")
            return True
        
        logger.info("Loaded {n} records into memory".format(n=len(records)))
        
        # Step 2: Extract all apple_ids
        apple_ids = [record['apple_id'] for record in records]
        logger.info("Apple IDs to process: {ids}".format(ids=apple_ids))
        
        # Step 3: Call API with all apple_ids at once
        logger.info("Calling Apple API with all IDs...")
        api_results = call_apple_lookup_api_batch(apple_ids)
        
        # Step 4: Process each record against API results
        logger.info("Comparing source data with API results...")
        
        last_apple_id_in_batch = None
        
        for idx, record in enumerate(records, 1):
            if shutdown_requested:
                logger.warning("Shutdown requested, stopping")
                break
            
            record_id = record['id']
            apple_id = record['apple_id']
            last_apple_id_in_batch = apple_id
            
            logger.info("[{idx}/{total}] Processing ID {rid} (Apple ID: {aid})".format(idx=idx, total=len(records), rid=record_id, aid=apple_id))
            
            # Get API result for this apple_id
            api_result = api_results.get(apple_id)
            
            if not api_result or not api_result.get('success'):
                error_msg = api_result.get('error', 'Unknown') if api_result else 'No API result'
                logger.error("API call failed for Apple ID {id}: {err}".format(id=apple_id, err=error_msg))
                total_errors += 1
                continue
            
            # Check for empty response
            if api_result.get('empty', False):
                save_empty_response_to_table(cursor, connection, apple_id)
                total_empty += 1
                continue
            
            # Detect changes
            has_changes, new_values = detect_changes(record, api_result['data'], COLUMNS_TO_CHECK)
            
            if has_changes:
                save_changes_to_table(cursor, connection, apple_id, new_values)
                total_changes += 1
            else:
                logger.info("No changes detected for ID {rid} (Apple ID: {aid})".format(rid=record_id, aid=apple_id))
                total_no_change += 1
        
        # Step 5: Save checkpoint with the last apple_id processed
        if last_apple_id_in_batch:
            save_checkpoint(cursor, connection, last_apple_id_in_batch)
        
        logger.info("=== BATCH PROCESSING COMPLETE ===")
        logger.info("Total Processed: {n}".format(n=len(records)))
        logger.info("Changes Detected: {n}".format(n=total_changes))
        logger.info("No Changes: {n}".format(n=total_no_change))
        logger.info("Empty Responses: {n}".format(n=total_empty))
        logger.info("Errors: {n}".format(n=total_errors))
        logger.info("Last Processed Apple ID: {id}".format(id=last_apple_id_in_batch))
        
        return True
        
    except Exception as e:
        logger.error("Error during batch processing: {err}".format(err=e), exc_info=True)
        return False
    finally:
        cursor.close()

# ==========================================
# MAIN ENTRY POINT
# ==========================================

def main():
    """Main entry point"""
    
    logger.info("=== APPLE PODCAST BATCH PROCESSOR STARTED ===")
    
    # Get target date from command line or use today
    target_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
    
    # Connect to database
    connection = get_db_connection()
    
    try:
        success = process_batch(connection, target_date)
        if success:
            logger.info("Script completed successfully")
        else:
            logger.error("Script completed with errors")
            sys.exit(1)
    except Exception as e:
        logger.error("Fatal error: {err}".format(err=e), exc_info=True)
        sys.exit(1)
    finally:
        if connection.is_connected():
            connection.close()
            logger.info("Database connection closed")
    
    logger.info("=== SCRIPT FINISHED ===")

if __name__ == "__main__":
    main()
