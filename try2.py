#!/usr/bin/env python3
# first try for script 
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
BATCH_SIZE = 10
API_ENDPOINT = "https://itunes.apple.com/lookup"  # Apple iTunes Lookup API
API_TIMEOUT = 30
DELAY_BETWEEN_REQUESTS = 0.5  # Apple allows higher rates

# Table names
SOURCE_TABLE = "apple_podcast_live_updates"  # Your source table with apple podcast data
CHANGES_TABLE = "apple_ive_derived_table"  # Table B: Detected changes
EMPTY_RESPONSE_TABLE = "apple_live_table_skipped"  # Table D: Empty API responses
CHECKPOINT_TABLE = "apple_ids_derived_index"  # Table C: Last processed ID

# Column mapping: DB column -> API response field
COLUMN_MAPPING = {
    'apple_podcast_url': 'collectionViewUrl',
    'category': 'primaryGenreName',
    'image': 'artworkUrl100',
    'name': 'collectionName',
    'artistName': 'artistName',
    'feed_url': 'feedUrl',
    'apple_id': 'collectionId'
}

# Columns to check for changes (excluding apple_id which is the key)
COLUMNS_TO_CHECK = ['apple_podcast_url', 'category', 'image', 'name', 'artistName', 'feed_url']

# ==========================================
# LOGGING SETUP
# ==========================================

# Create logs directory if it doesn't exist
import os
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

last_processed_id = 0
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

def ensure_tables_exist(cursor):
    """Create required tables if they don't exist"""
    
    # Table B: Changes table
    create_changes_table = f"""
    CREATE TABLE IF NOT EXISTS `{CHANGES_TABLE}` (
        id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        source_record_id INT UNSIGNED NOT NULL,
        apple_id BIGINT NOT NULL,
        changes_detected TEXT NOT NULL,
        detected_at DATETIME NOT NULL,
        INDEX idx_source_id (source_record_id),
        INDEX idx_apple_id (apple_id),
        INDEX idx_detected_at (detected_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci
    """
    
    # Table D: Empty responses table
    create_empty_table = f"""
    CREATE TABLE IF NOT EXISTS `{EMPTY_RESPONSE_TABLE}` (
        id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        source_record_id INT UNSIGNED NOT NULL,
        apple_id BIGINT NOT NULL,
        reason VARCHAR(255) DEFAULT NULL,
        recorded_at DATETIME NOT NULL,
        INDEX idx_source_id (source_record_id),
        INDEX idx_apple_id (apple_id),
        INDEX idx_recorded_at (recorded_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci
    """
    
    # Table C: Checkpoint table
    create_checkpoint_table = f"""
    CREATE TABLE IF NOT EXISTS `{CHECKPOINT_TABLE}` (
        id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        last_processed_id INT UNSIGNED NOT NULL,
        last_run_date DATETIME NOT NULL,
        INDEX idx_last_run (last_run_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci
    """
    
    try:
        cursor.execute(create_changes_table)
        cursor.execute(create_empty_table)
        cursor.execute(create_checkpoint_table)
        logger.info("All required tables verified/created successfully")
        return True
    except Error as e:
        logger.error(f"Failed to create tables: {e}")
        return False

def get_last_checkpoint(cursor) -> int:
    """Retrieve the last processed ID from checkpoint table"""
    try:
        query = f"""
        SELECT appleID, updateTime 
        FROM `{CHECKPOINT_TABLE}` 
        ORDER BY id DESC 
        LIMIT 1
        """
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result:
            last_id, last_run = result
            logger.info(f"Found checkpoint: Last ID = {last_id}, Last Run = {last_run}")
            return last_id
        else:
            logger.info("No checkpoint found, starting from beginning")
            return 0
    except Error as e:
        logger.error(f"Error fetching checkpoint: {e}")
        return 0

def save_checkpoint(cursor, connection, last_id: int) -> bool:
    """Save the current processing checkpoint"""
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        query = f"""
        INSERT INTO `{CHECKPOINT_TABLE}` (appleId, updateTime) 
        VALUES (%s, %s)
        """
        cursor.execute(query, (last_id, now))
        connection.commit()
        logger.info(f"Checkpoint saved: Last processed ID = {last_id}")
        return True
    except Error as e:
        logger.error(f"Failed to save checkpoint: {e}")
        return False

def fetch_batch_records(cursor, last_id: int, batch_size: int, target_date: str) -> List[Dict]:
    """Fetch a batch of records from the source table"""
    try:
        columns = ', '.join(['id'] + COLUMNS_TO_CHECK + ['apple_id'])
        
        # Modify date condition based on your date column name
        query = f"""
        SELECT {columns} 
        FROM `{SOURCE_TABLE}` 
        WHERE id > %s 
        AND DATE(updated_at) > ? 
        AND apple_id IS NOT NULL
        ORDER BY id ASC 
        LIMIT %s
        """
        
        cursor.execute(query, (last_id, target_date, batch_size))
        
        # Fetch all rows and convert to list of dicts
        columns_list = [desc[0] for desc in cursor.description]
        records = []
        
        for row in cursor.fetchall():
            record = dict(zip(columns_list, row))
            records.append(record)
        
        logger.info(f"Fetched {len(records)} records from ID {last_id}")
        return records
        
    except Error as e:
        logger.error(f"Failed to fetch batch records: {e}")
        return []

# ==========================================
# API FUNCTIONS
# ==========================================

def call_apple_lookup_api(apple_id: int) -> Dict[str, Any]:
    """Call the Apple iTunes Lookup API for a given apple_id"""
    try:
        params = {
            'id': apple_id,
            'entity': 'podcast'
        }
        
        response = requests.get(
            API_ENDPOINT,
            params=params,
            timeout=API_TIMEOUT
        )
        
        if response.status_code != 200:
            logger.warning(f"API returned HTTP {response.status_code} for Apple ID {apple_id}")
            return {
                'success': False, 
                'error': f'HTTP {response.status_code}',
                'http_code': response.status_code
            }
        
        data = response.json()
        
        # Check if results exist
        if 'results' not in data or len(data['results']) == 0:
            logger.warning(f"Empty results from API for Apple ID {apple_id}")
            return {'success': True, 'empty': True}
        
        # Return the first result (should be the podcast)
        podcast_data = data['results'][0]
        return {'success': True, 'data': podcast_data, 'empty': False}
        
    except requests.exceptions.Timeout:
        logger.error(f"API timeout for Apple ID {apple_id}")
        return {'success': False, 'error': 'Timeout'}
    
    except requests.exceptions.RequestException as e:
        logger.error(f"API call failed for Apple ID {apple_id}: {e}")
        return {'success': False, 'error': str(e)}
    
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON response for Apple ID {apple_id}")
        return {'success': False, 'error': 'Invalid JSON'}

# ==========================================
# CHANGE DETECTION FUNCTIONS
# ==========================================

def get_artwork_url(api_data: Dict) -> Optional[str]:
    """
    Get artwork URL from API response with fallback logic.
    Priority: artworkUrl100 -> artworkUrl60 -> artworkUrl30
    """
    artwork_fields = ['artworkUrl100', 'artworkUrl60', 'artworkUrl30']
    
    for field in artwork_fields:
        url = api_data.get(field)
        if url:
            logger.debug(f"Found artwork: {field} = {url}")
            return url
    
    logger.warning("No artwork URL found in API response")
    return None

def normalize_image_url(url: Optional[str]) -> Optional[str]:
    """
    Normalize image URL by removing size suffix.
    Example: https://...jpg/100x100bb.jpg -> https://...jpg
    """
    if not url:
        return None
    
    # Remove everything after the first .jpg (including the .jpg itself, then add it back)
    if '.jpg' in url:
        base_url = url.split('.jpg')[0] + '.jpg'
        logger.debug(f"Normalized URL: {url} -> {base_url}")
        return base_url
    
    # If no .jpg found, return as-is
    return url

def map_api_to_db_columns(api_data: Dict) -> Dict:
    """Map API response fields to database column names"""
    mapped_data = {}
    
    for db_column, api_field in COLUMN_MAPPING.items():
        if db_column == 'image':
            # Special handling for image field
            mapped_data[db_column] = get_artwork_url(api_data)
        else:
            mapped_data[db_column] = api_data.get(api_field)
    
    return mapped_data

def detect_changes(old_record: Dict, api_data: Dict, columns: List[str]) -> Dict[str, Dict]:
    """Detect changes between database record and API response"""
    changes = {}
    
    # Map API data to DB column names
    new_data = map_api_to_db_columns(api_data)
    
    for column in columns:
        old_value = old_record.get(column)
        new_value = new_data.get(column)
        
        # Special handling for image URLs
        if column == 'image':
            # Normalize both URLs before comparison
            old_normalized = normalize_image_url(old_value)
            new_normalized = normalize_image_url(new_value)
            
            if old_normalized != new_normalized:
                changes[column] = {
                    'old': old_value,
                    'new': new_value,
                    'old_normalized': old_normalized,
                    'new_normalized': new_normalized,
                    'api_field': COLUMN_MAPPING.get(column, column)
                }
        else:
            # Convert values to strings for comparison to handle None and type differences
            old_str = str(old_value) if old_value is not None else None
            new_str = str(new_value) if new_value is not None else None
            
            # Compare values
            if old_str != new_str:
                changes[column] = {
                    'old': old_value,
                    'new': new_value,
                    'api_field': COLUMN_MAPPING.get(column, column)
                }
    
    return changes

def save_changes_to_table_b(cursor, connection, record_id: int, apple_id: int, changes: Dict) -> bool:
    """Save detected changes to Table B"""
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        changes_json = json.dumps(changes, ensure_ascii=False)
        
        query = f"""
        INSERT INTO `{CHANGES_TABLE}` 
        (source_record_id, apple_id, changes_detected, detected_at) 
        VALUES (%s, %s, %s, %s)
        """
        
        cursor.execute(query, (record_id, apple_id, changes_json, now))
        connection.commit()
        
        logger.info(f"Changes saved for ID {record_id} (Apple ID: {apple_id}): {len(changes)} columns changed")
        logger.info(f"Changed columns: {', '.join(changes.keys())}")
        return True
        
    except Error as e:
        logger.error(f"Failed to save changes for ID {record_id}: {e}")
        return False

def save_empty_response_to_table_d(cursor, connection, record_id: int, apple_id: int, reason: str = 'Empty API response') -> bool:
    """Save empty response record to Table D"""
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        query = f"""
        INSERT INTO `{EMPTY_RESPONSE_TABLE}` 
        (source_record_id, apple_id, reason, recorded_at) 
        VALUES (%s, %s, %s, %s)
        """
        
        cursor.execute(query, (record_id, apple_id, reason, now))
        connection.commit()
        
        logger.info(f"Empty response recorded for ID {record_id} (Apple ID: {apple_id})")
        return True
        
    except Error as e:
        logger.error(f"Failed to save empty response for ID {record_id}: {e}")
        return False

# ==========================================
# MAIN PROCESSING FUNCTION
# ==========================================

def process_batch(connection, target_date: str) -> bool:
    """Main batch processing function"""
    global last_processed_id, shutdown_requested
    
    logger.info("=== STARTING APPLE PODCAST LOOKUP PROCESSING ===")
    logger.info(f"Target Date: {target_date}")
    logger.info(f"Batch Size: {BATCH_SIZE}")
    
    cursor = connection.cursor()
    
    # Ensure tables exist
    if not ensure_tables_exist(cursor):
        logger.error("Failed to verify/create required tables")
        return False
    
    # Get last checkpoint
    last_processed_id = get_last_checkpoint(cursor)
    
    total_processed = 0
    total_changes = 0
    total_empty = 0
    total_errors = 0
    total_no_change = 0
    
    try:
        while not shutdown_requested:
            # Fetch batch
            records = fetch_batch_records(cursor, last_processed_id, BATCH_SIZE, target_date)
            
            if not records:
                logger.info("No more records to process")
                break
            
            logger.info(f"Processing batch of {len(records)} records")
            
            for record in records:
                if shutdown_requested:
                    logger.warning("Shutdown requested, stopping after current record")
                    break
                
                record_id = record['id']
                apple_id = record['apple_id']
                last_processed_id = record_id
                total_processed += 1
                
                logger.info(f"[{total_processed}] Processing ID {record_id} (Apple ID: {apple_id})")
                
                # Call API
                api_result = call_apple_lookup_api(apple_id)
                
                if not api_result['success']:
                    error_msg = api_result.get('error', 'Unknown')
                    logger.error(f"API call failed for ID {record_id} (Apple ID: {apple_id}): {error_msg}")
                    total_errors += 1
                    continue
                
                # Check for empty response
                if api_result.get('empty', False):
                    save_empty_response_to_table_d(cursor, connection, record_id, apple_id, 'No results from Apple API')
                    total_empty += 1
                    continue
                
                # Detect changes
                changes = detect_changes(record, api_result['data'], COLUMNS_TO_CHECK)
                
                if changes:
                    save_changes_to_table_b(cursor, connection, record_id, apple_id, changes)
                    total_changes += 1
                else:
                    logger.info(f"No changes detected for ID {record_id} (Apple ID: {apple_id})")
                    total_no_change += 1
                
                # Delay between requests
                if DELAY_BETWEEN_REQUESTS > 0:
                    time.sleep(DELAY_BETWEEN_REQUESTS)
            
            # Save checkpoint after each batch
            save_checkpoint(cursor, connection, last_processed_id)
            
            logger.info(f"Batch completed. Progress: Processed={total_processed}, Changes={total_changes}, No Change={total_no_change}, Empty={total_empty}, Errors={total_errors}")
            
            if shutdown_requested:
                break
        
        # Final checkpoint save
        save_checkpoint(cursor, connection, last_processed_id)
        
        logger.info("=== PROCESSING COMPLETE ===")
        logger.info(f"Total Processed: {total_processed}")
        logger.info(f"Changes Detected: {total_changes}")
        logger.info(f"No Changes: {total_no_change}")
        logger.info(f"Empty Responses: {total_empty}")
        logger.info(f"Errors: {total_errors}")
        logger.info(f"Last Processed ID: {last_processed_id}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error during batch processing: {e}")
        save_checkpoint(cursor, connection, last_processed_id)
        return False
    finally:
        cursor.close()

# ==========================================
# MAIN ENTRY POINT
# ==========================================

def main():
    """Main entry point"""
    global last_processed_id
    
    logger.info("=== APPLE PODCAST LOOKUP PROCESSOR STARTED ===")
    
    # Get target date from command line or use today
    target_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
    
    # Connect to database
    connection = get_db_connection()
    
    try:
        process_batch(connection, target_date)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        cursor = connection.cursor()
        save_checkpoint(cursor, connection, last_processed_id)
        cursor.close()
    finally:
        if connection.is_connected():
            connection.close()
            logger.info("Database connection closed")
    
    logger.info("Script finished")

if __name__ == "__main__":
    main()