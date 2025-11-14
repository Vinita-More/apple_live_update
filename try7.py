#!/usr/bin/env python3
# Batch processing script for Apple Podcast data comparison
import sys
import json
import time
import signal
import logging
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import mysql.connector
from mysql.connector import Error
import password
import os

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

# Table names (assumed to already exist in the DB)
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
log_dir = "podcast_logs"
if not os.path.exists(log_dir):
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
    logger.warning(f"Received shutdown signal ({signum}). Will stop after current batch.")
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
        logger.error(f"Database connection failed: {e}")
        sys.exit(1)

def ensure_tables_exist(cursor) -> bool:
    """
    NO-OP verifier.

    Previously this function created tables if missing. Per your request,
    it now does NOT create or modify any schema. It simply returns True.
    """
    logger.info("Skipping table creation/alteration — assuming all tables already exist.")
    return True

def get_last_checkpoint(cursor) -> int:
    """Retrieve the last processed apple_id from checkpoint table"""
    try:
        query = f"""
        SELECT appleId, updateTime 
        FROM `{CHECKPOINT_TABLE}` 
        ORDER BY updateTime DESC 
        LIMIT 1
        """
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result:
            last_apple_id, last_run = result
            logger.info(f"Found checkpoint: Last Apple ID = {last_apple_id}, Last Run = {last_run}")
            try:
                return int(last_apple_id) if last_apple_id else 0
            except (ValueError, TypeError):
                logger.warning("Checkpoint appleId is not an integer-like value; returning 0")
                return 0
        else:
            logger.info("No checkpoint found, starting from beginning")
            return 0
    except Error as e:
        logger.error(f"Error fetching checkpoint: {e}")
        return 0

def save_checkpoint(cursor, connection, apple_id: int) -> bool:
    """Save the current processing checkpoint using UPSERT"""
    try:
        query = f"""
        INSERT INTO `{CHECKPOINT_TABLE}` (appleId) 
        VALUES (%s)
        ON DUPLICATE KEY UPDATE
        updateTime = CURRENT_TIMESTAMP
        """
        cursor.execute(query, (str(apple_id),))
        connection.commit()
        logger.info(f"Checkpoint saved: Apple ID = {apple_id}")
        return True
    except Error as e:
        logger.error(f"Failed to save checkpoint: {e}")
        return False

def fetch_batch_records(cursor, last_apple_id: int, batch_size: int, target_date: str) -> List[Dict]:
    """
    Fetch a batch of records from the source table using apple_id as the record id.
    - Does NOT expect an 'id' column.
    - Requires 'apple_id' to exist in the source table.
    - Respects the DATE(updated_at) > target_date filter.
    """
    try:
        # Columns we want to fetch (no 'id')
        desired_cols = COLUMNS_TO_CHECK + ['apple_id']

        # Verify which of these columns actually exist in the source table
        info_query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
        """
        cursor.execute(info_query, (DB_CONFIG['database'], SOURCE_TABLE))
        existing_cols = {row[0] for row in cursor.fetchall()}

        # Keep only columns that exist
        selected_cols = [col for col in desired_cols if col in existing_cols]

        if 'apple_id' not in selected_cols:
            logger.error("'apple_id' column not found in source table. Cannot proceed.")
            return []

        columns_sql = ', '.join(selected_cols)

        query = f"""
        SELECT {columns_sql}
        FROM `{SOURCE_TABLE}`
        WHERE apple_id > %s
          AND DATE(updated_at) > %s
          AND apple_id IS NOT NULL
        ORDER BY apple_id ASC
        LIMIT %s
        """

        cursor.execute(query, (last_apple_id, target_date, batch_size))

        columns_list = [desc[0] for desc in cursor.description]
        records = [dict(zip(columns_list, row)) for row in cursor.fetchall()]

        logger.info(f"Fetched {len(records)} records starting from Apple ID {last_apple_id}")
        return records

    except Error as e:
        logger.error(f"Failed to fetch batch records: {e}")
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
        
        logger.info(f"Calling API with {len(apple_ids)} apple_ids: {apple_ids}")
        
        response = requests.get(
            API_ENDPOINT,
            params=params,
            timeout=API_TIMEOUT
        )
        
        if response.status_code != 200:
            logger.warning(f"API returned HTTP {response.status_code}")
            # Return error for all IDs
            return {
                apple_id: {
                    'success': False, 
                    'error': f'HTTP {response.status_code}',
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
            
            logger.info(f"API returned {len(results_map)} results")
        
        # Mark any IDs that didn't return results as empty
        for apple_id in apple_ids:
            if apple_id not in results_map:
                results_map[apple_id] = {
                    'success': True,
                    'empty': True
                }
                logger.warning(f"No result returned for Apple ID {apple_id}")
        
        return results_map
        
    except requests.exceptions.Timeout:
        logger.error(f"API timeout for batch request")
        return {apple_id: {'success': False, 'error': 'Timeout'} for apple_id in apple_ids}
    
    except requests.exceptions.RequestException as e:
        logger.error(f"API call failed: {e}")
        return {apple_id: {'success': False, 'error': str(e)} for apple_id in apple_ids}
    
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON response")
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

def strip_query(url: Optional[str]) -> Optional[str]:
    """Remove query parameters from URL (everything after '?')"""
    if not url:
        return url
    return url.split('?', 1)[0]

def detect_changes(old_record: Dict[str, Any], api_data: Dict[str, Any], columns: List[str]) -> Tuple[bool, Dict[str, Any]]:
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
                logger.debug(f"Change detected in {source_col}: {old_value} -> {new_value}")
        else:
            old_str = str(old_value) if old_value is not None else None
            new_str = str(new_value) if new_value is not None else None
            
            # --- IMPORTANT: Only strip query params from API value (new_str) ---
            if source_col in ['apple_podcast_url', 'feed_url'] and new_str:
                new_str = strip_query(new_str)
            
            if old_str != new_str:
                has_changes = True
                logger.debug(f"Change detected in {source_col}: {old_value} -> {new_value}")
    
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
        update_clause = ', '.join([f"`{col}` = VALUES(`{col}`)" for col in new_values.keys()])
        
        query = f"""
        INSERT INTO `{CHANGES_TABLE}` 
        (`{'`, `'.join(columns)}`) 
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
        {update_clause},
        updateDate = CURRENT_TIMESTAMP
        """
        
        # Prepare values: appleId + all new values
        values = [str(apple_id)] + list(new_values.values())
        
        cursor.execute(query, values)
        connection.commit()
        
        logger.info(f"Updated changes table for Apple ID {apple_id}: {len(new_values)} columns")
        return True
        
    except Error as e:
        logger.error(f"Failed to save changes for Apple ID {apple_id}: {e}")
        return False

def save_empty_response_to_table(cursor, connection, apple_id: int) -> bool:
    """
    Save/update empty response record to skipped table using UPSERT.
    Increments retryCount each time.
    """
    try:
        query = f"""
        INSERT INTO `{EMPTY_RESPONSE_TABLE}` 
        (appleId, retryCount) 
        VALUES (%s, 1)
        ON DUPLICATE KEY UPDATE
        retryCount = retryCount + 1
        """
        
        cursor.execute(query, (str(apple_id),))
        connection.commit()
        
        # Get the current retry count
        cursor.execute(f"SELECT retryCount FROM `{EMPTY_RESPONSE_TABLE}` WHERE appleId = %s", (str(apple_id),))
        result = cursor.fetchone()
        retry_count = result[0] if result else 1
        
        logger.info(f"Empty response recorded for Apple ID {apple_id} (retry count: {retry_count})")
        return True
        
    except Error as e:
        logger.error(f"Failed to save empty response for Apple ID {apple_id}: {e}")
        return False

# ==========================================
# MAIN PROCESSING FUNCTION
# ==========================================

def process_batch(connection, target_date: str) -> bool:
    """Main batch processing function - processes exactly BATCH_SIZE records then exits"""
    global shutdown_requested
    
    logger.info("=== STARTING APPLE PODCAST BATCH PROCESSING ===")
    logger.info(f"Target Date: {target_date}")
    logger.info(f"Batch Size: {BATCH_SIZE}")
    
    cursor = connection.cursor()
    
    # Ensure tables exist (NO-OP now)
    if not ensure_tables_exist(cursor):
        logger.error("Table verification failed (ensure_tables_exist returned False). Aborting.")
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
        
        logger.info(f"Loaded {len(records)} records into memory")
        
        # Step 2: Extract all apple_ids (validate and coerce to int where possible)
        apple_ids = []
        valid_records = []
        for rec in records:
            raw = rec.get('apple_id')
            if raw is None:
                logger.warning("Skipping a row with missing apple_id")
                continue
            try:
                aid = int(raw)
            except (ValueError, TypeError):
                logger.warning(f"Skipping row with non-integer apple_id: {raw}")
                continue
            apple_ids.append(aid)
            valid_records.append(rec)
        
        if not apple_ids:
            logger.info("No valid apple_id values found in fetched rows. Nothing to process.")
            return True
        
        logger.info(f"Apple IDs to process: {apple_ids}")
        
        # Step 3: Call API with all apple_ids at once
        logger.info("Calling Apple API with all IDs...")
        api_results = call_apple_lookup_api_batch(apple_ids)
        
        # Step 4: Process each valid record against API results
        logger.info("Comparing source data with API results...")
        
        last_apple_id_in_batch = None
        
        for idx, record in enumerate(valid_records, 1):
            if shutdown_requested:
                logger.warning("Shutdown requested, stopping")
                break
            
            # Use apple_id as the identifier (no 'id' column assumed)
            raw_apple_id = record.get('apple_id')
            try:
                apple_id = int(raw_apple_id)
            except (ValueError, TypeError):
                logger.warning(f"Skipping record at index {idx} due to invalid apple_id: {raw_apple_id}")
                continue
            
            last_apple_id_in_batch = apple_id
            
            logger.info(f"[{idx}/{len(valid_records)}] Processing (Apple ID: {apple_id})")
            
            # Get API result for this apple_id
            api_result = api_results.get(apple_id)
            
            if not api_result or not api_result.get('success'):
                error_msg = api_result.get('error', 'Unknown') if api_result else 'No API result'
                logger.error(f"API call failed for Apple ID {apple_id}: {error_msg}")
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
                logger.info(f"No changes detected for Apple ID: {apple_id}")
                total_no_change += 1
        
        # Step 5: Save checkpoint with the last apple_id processed
        if last_apple_id_in_batch:
            save_checkpoint(cursor, connection, last_apple_id_in_batch)
        
        logger.info("=== BATCH PROCESSING COMPLETE ===")
        logger.info(f"Total Processed: {len(valid_records)}")
        logger.info(f"Changes Detected: {total_changes}")
        logger.info(f"No Changes: {total_no_change}")
        logger.info(f"Empty Responses: {total_empty}")
        logger.info(f"Errors: {total_errors}")
        logger.info(f"Last Processed Apple ID: {last_apple_id_in_batch}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error during batch processing: {e}", exc_info=True)
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
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if connection.is_connected():
            connection.close()
            logger.info("Database connection closed")
    
    logger.info("=== SCRIPT FINISHED ===")

if __name__ == "__main__":
    main()
